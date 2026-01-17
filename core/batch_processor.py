#!/usr/bin/env python3
"""
BATCH PROCESSOR - PROCESAMIENTO POR LOTES OPTIMIZADO
====================================================
Sistema de procesamiento por lotes para ETL con soporte para:
- Procesamiento paralelo
- Manejo de memoria eficiente
- Checkpoints y recuperación
- Monitoreo y logging detallado
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Optional, Any
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import time
import psutil
import json
from pathlib import Path
import logging


@dataclass
class BatchConfig:
    """Configuración del procesador por lotes"""

    chunk_size: int = 1000
    max_workers: int = 4
    timeout: int = 300
    max_retries: int = 3
    retry_delay: int = 5
    max_memory_mb: int = 512
    enable_checkpoints: bool = True
    checkpoint_interval: int = 100


@dataclass
class BatchResult:
    """Resultado del procesamiento por lotes"""

    batch_id: int
    records_processed: int
    records_failed: int
    execution_time: float
    memory_used_mb: float
    status: str  # 'success', 'partial', 'failed'
    errors: List[str]
    metadata: Dict[str, Any]


class BatchProcessor:
    """
    Procesador por lotes con capacidades avanzadas
    """

    def __init__(self, config: BatchConfig, checkpoint_dir: Path = None):
        self.config = config
        self.checkpoint_dir = checkpoint_dir or Path("data/checkpoints")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(__name__)
        self.results: List[BatchResult] = []
        self.total_processed = 0
        self.total_failed = 0

    def process_dataframe(
        self, df: pd.DataFrame, process_func: Callable, job_name: str = "batch_job"
    ) -> List[BatchResult]:
        """
        Procesa un DataFrame en lotes con procesamiento paralelo

        Args:
            df: DataFrame a procesar
            process_func: Función que procesa cada lote
            job_name: Nombre del trabajo (para checkpoints)

        Returns:
            Lista de resultados por lote
        """
        self.logger.info(f"🚀 Iniciando procesamiento por lotes: {job_name}")
        self.logger.info(f"   Total registros: {len(df):,}")
        self.logger.info(f"   Tamaño de lote: {self.config.chunk_size}")
        self.logger.info(f"   Workers: {self.config.max_workers}")

        start_time = time.time()

        # Dividir DataFrame en chunks
        chunks = self._split_dataframe(df)
        total_chunks = len(chunks)

        self.logger.info(f"   Total de lotes: {total_chunks}")

        # Verificar si hay checkpoint previo
        start_chunk = self._get_checkpoint(job_name)
        if start_chunk > 0:
            self.logger.info(f"   📍 Reanudando desde lote {start_chunk}")
            chunks = chunks[start_chunk:]

        # Procesar chunks en paralelo
        results = []

        with ProcessPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(
                    self._process_chunk_with_retry,
                    chunk_id + start_chunk,
                    chunk,
                    process_func,
                ): chunk_id
                + start_chunk
                for chunk_id, chunk in enumerate(chunks)
            }

            # Process completed futures
            for future in as_completed(future_to_chunk):
                chunk_id = future_to_chunk[future]

                try:
                    result = future.result(timeout=self.config.timeout)
                    results.append(result)

                    self.total_processed += result.records_processed
                    self.total_failed += result.records_failed

                    # Guardar checkpoint
                    if self.config.enable_checkpoints:
                        self._save_checkpoint(job_name, chunk_id + 1)

                    # Log progress
                    progress = (len(results) / total_chunks) * 100
                    self.logger.info(
                        f"   ✓ Lote {chunk_id}/{total_chunks} "
                        f"({progress:.1f}%) - "
                        f"{result.records_processed} registros - "
                        f"{result.execution_time:.2f}s"
                    )

                except Exception as e:
                    self.logger.error(f"   ✗ Error en lote {chunk_id}: {e}")
                    results.append(
                        BatchResult(
                            batch_id=chunk_id,
                            records_processed=0,
                            records_failed=len(chunks[chunk_id - start_chunk]),
                            execution_time=0,
                            memory_used_mb=0,
                            status="failed",
                            errors=[str(e)],
                            metadata={},
                        )
                    )

        # Calcular estadísticas finales
        elapsed_time = time.time() - start_time

        self.logger.info(f"\n✅ Procesamiento completado")
        self.logger.info(f"   ⏱️  Tiempo total: {elapsed_time:.2f}s")
        self.logger.info(f"   ✓ Registros procesados: {self.total_processed:,}")
        self.logger.info(f"   ✗ Registros fallidos: {self.total_failed:,}")
        self.logger.info(
            f"   📊 Tasa de éxito: {(self.total_processed/(self.total_processed+self.total_failed)*100):.1f}%"
        )

        # Limpiar checkpoint si fue exitoso
        if self.total_failed == 0:
            self._clear_checkpoint(job_name)

        self.results = results
        return results

    def _split_dataframe(self, df: pd.DataFrame) -> List[pd.DataFrame]:
        """Divide DataFrame en chunks"""
        chunks = []
        for i in range(0, len(df), self.config.chunk_size):
            chunk = df.iloc[i : i + self.config.chunk_size].copy()
            chunks.append(chunk)
        return chunks

    def _process_chunk_with_retry(
        self, chunk_id: int, chunk: pd.DataFrame, process_func: Callable
    ) -> BatchResult:
        """Procesa un chunk con reintentos en caso de error"""

        for attempt in range(self.config.max_retries):
            try:
                return self._process_chunk(chunk_id, chunk, process_func)

            except Exception as e:
                if attempt < self.config.max_retries - 1:
                    self.logger.warning(
                        f"Reintento {attempt + 1}/{self.config.max_retries} "
                        f"para lote {chunk_id}: {e}"
                    )
                    time.sleep(self.config.retry_delay)
                else:
                    raise

    def _process_chunk(
        self, chunk_id: int, chunk: pd.DataFrame, process_func: Callable
    ) -> BatchResult:
        """Procesa un chunk individual"""

        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024

        errors = []
        processed = 0
        failed = 0

        try:
            # Ejecutar función de procesamiento
            result = process_func(chunk)

            if isinstance(result, pd.DataFrame):
                processed = len(result)
            elif isinstance(result, dict):
                processed = result.get("processed", 0)
                failed = result.get("failed", 0)
                errors = result.get("errors", [])
            else:
                processed = len(chunk)

            status = "success" if failed == 0 else "partial"

        except Exception as e:
            errors.append(str(e))
            failed = len(chunk)
            status = "failed"

        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024

        return BatchResult(
            batch_id=chunk_id,
            records_processed=processed,
            records_failed=failed,
            execution_time=end_time - start_time,
            memory_used_mb=end_memory - start_memory,
            status=status,
            errors=errors,
            metadata={
                "chunk_size": len(chunk),
                "timestamp": datetime.now().isoformat(),
            },
        )

    def _save_checkpoint(self, job_name: str, chunk_id: int):
        """Guarda checkpoint para recuperación"""
        checkpoint_file = self.checkpoint_dir / f"{job_name}.checkpoint"

        checkpoint_data = {
            "job_name": job_name,
            "chunk_id": chunk_id,
            "timestamp": datetime.now().isoformat(),
            "total_processed": self.total_processed,
            "total_failed": self.total_failed,
        }

        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f, indent=2)

    def _get_checkpoint(self, job_name: str) -> int:
        """Recupera checkpoint si existe"""
        checkpoint_file = self.checkpoint_dir / f"{job_name}.checkpoint"

        if not checkpoint_file.exists():
            return 0

        try:
            with open(checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
                return checkpoint_data.get("chunk_id", 0)
        except:
            return 0

    def _clear_checkpoint(self, job_name: str):
        """Elimina checkpoint después de éxito"""
        checkpoint_file = self.checkpoint_dir / f"{job_name}.checkpoint"
        if checkpoint_file.exists():
            checkpoint_file.unlink()

    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumen de la ejecución"""
        if not self.results:
            return {}

        total_time = sum(r.execution_time for r in self.results)
        total_memory = sum(r.memory_used_mb for r in self.results)

        return {
            "total_batches": len(self.results),
            "total_processed": self.total_processed,
            "total_failed": self.total_failed,
            "success_rate": (
                (
                    self.total_processed
                    / (self.total_processed + self.total_failed)
                    * 100
                )
                if (self.total_processed + self.total_failed) > 0
                else 0
            ),
            "total_time": total_time,
            "avg_time_per_batch": total_time / len(self.results),
            "total_memory_mb": total_memory,
            "avg_memory_per_batch_mb": total_memory / len(self.results),
            "status": "success" if self.total_failed == 0 else "completed_with_errors",
        }


class StreamingBatchProcessor(BatchProcessor):
    """
    Procesador por lotes con streaming para datasets muy grandes
    que no caben en memoria
    """

    def process_large_file(
        self,
        file_path: Path,
        process_func: Callable,
        job_name: str = "streaming_job",
        file_format: str = "csv",
    ) -> List[BatchResult]:
        """
        Procesa un archivo grande en streaming por lotes

        Args:
            file_path: Ruta al archivo
            process_func: Función de procesamiento
            job_name: Nombre del trabajo
            file_format: Formato del archivo (csv, parquet)
        """
        self.logger.info(f"🌊 Iniciando procesamiento streaming: {file_path}")

        start_time = time.time()
        results = []
        chunk_id = 0

        # Recuperar checkpoint
        start_chunk = self._get_checkpoint(job_name)

        if file_format == "csv":
            reader = pd.read_csv(
                file_path, chunksize=self.config.chunk_size, iterator=True
            )
        elif file_format == "parquet":
            # Para parquet, leer en chunks manualmente
            df = pd.read_parquet(file_path)
            reader = self._split_dataframe(df)
        else:
            raise ValueError(f"Formato no soportado: {file_format}")

        for chunk in reader:
            # Skip hasta el checkpoint
            if chunk_id < start_chunk:
                chunk_id += 1
                continue

            try:
                result = self._process_chunk_with_retry(chunk_id, chunk, process_func)
                results.append(result)

                self.total_processed += result.records_processed
                self.total_failed += result.records_failed

                # Checkpoint
                if (
                    self.config.enable_checkpoints
                    and chunk_id % self.config.checkpoint_interval == 0
                ):
                    self._save_checkpoint(job_name, chunk_id + 1)

                # Log progress
                self.logger.info(
                    f"   ✓ Lote {chunk_id} - "
                    f"{result.records_processed} registros - "
                    f"{result.execution_time:.2f}s"
                )

            except Exception as e:
                self.logger.error(f"   ✗ Error en lote {chunk_id}: {e}")
                results.append(
                    BatchResult(
                        batch_id=chunk_id,
                        records_processed=0,
                        records_failed=len(chunk),
                        execution_time=0,
                        memory_used_mb=0,
                        status="failed",
                        errors=[str(e)],
                        metadata={},
                    )
                )

            chunk_id += 1

        elapsed_time = time.time() - start_time

        self.logger.info(f"\n✅ Procesamiento streaming completado")
        self.logger.info(f"   ⏱️  Tiempo total: {elapsed_time:.2f}s")
        self.logger.info(f"   ✓ Registros procesados: {self.total_processed:,}")

        # Limpiar checkpoint si exitoso
        if self.total_failed == 0:
            self._clear_checkpoint(job_name)

        self.results = results
        return results
