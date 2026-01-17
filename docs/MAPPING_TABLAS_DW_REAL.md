# MAPPING DE TABLAS - DATA WAREHOUSE PUNTA FINA
**Basado en la estructura real de datawarehouse_bi**

## Índice

### Tablas de Hechos (Facts)
1. [fact_ventas](#1-fact_ventas)
2. [fact_inventario](#2-fact_inventario)
3. [fact_transacciones](#3-fact_transacciones)
4. [fact_promocion_aplicada](#4-fact_promocion_aplicada)
5. [fact_balance](#5-fact_balance)
6. [fact_estado_resultados](#6-fact_estado_resultados)

### Tablas de Dimensiones (Dimensions)
7. [dim_fecha](#7-dim_fecha)
8. [dim_usuario](#8-dim_usuario)
9. [dim_producto](#9-dim_producto)
10. [dim_cliente](#10-dim_cliente)
11. [dim_orden](#11-dim_orden)
12. [dim_promocion](#12-dim_promocion)
13. [dim_impuestos](#13-dim_impuestos)
14. [dim_almacen](#14-dim_almacen)
15. [dim_proveedor](#15-dim_proveedor)
16. [dim_tipo_movimiento](#16-dim_tipo_movimiento)
17. [dim_cuenta_contable](#17-dim_cuenta_contable)
18. [dim_centro_costo](#18-dim_centro_costo)
19. [dim_tipo_transaccion](#19-dim_tipo_transaccion)
20. [dim_periodo](#20-dim_periodo)

---

# TABLAS DE HECHOS

## 1. fact_ventas

**Tipo de tabla:** Hechos  
**Nombre:** fact_ventas  
**Nombre visual:** Ventas  
**Descripción:** Contiene los registros de cada línea de pedido efectuada por clientes finales. Es la base cuantitativa para análisis de ingresos, unidades vendidas, descuentos, costos y márgenes. Cada registro representa una línea de pedido única.  
**Valores nulos:** No se permiten nulos. Las claves foráneas deben estar garantizadas mediante ETL.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| venta_id | ID único de la fila en hechos (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| fecha_id | Fecha de creación del line item | INT | oro_order | created_at | Derivado. Se transforma a formato YYYYMMDD y cruza con dim_fecha |
| cliente_id | Cliente que realizó la compra | INT | oro_order | customer_id | ID directo de OroCommerce |
| producto_id | Producto vendido | INT | oro_order_line_item | product_id | ID directo de OroCommerce |
| orden_id | Orden asociada al line item | INT | oro_order_line_item | order_id | ID directo de OroCommerce |
| usuario_id | Usuario interno responsable | INT | oro_order | user_owner_id | FK a dim_usuario |
| almacen_id | Almacén de origen | INT | — | — | FK a dim_almacen (por defecto almacén principal) |
| cantidad | Número de unidades vendidas | NUMERIC(10,2) | oro_order_line_item | quantity | Directo desde la línea de la orden |
| precio_unitario | Precio por unidad | NUMERIC(10,2) | oro_order_line_item | value | Precio aplicado por unidad |
| subtotal | Subtotal después de descuentos | NUMERIC(10,2) | — | — | Calculado: (cantidad * precio_unitario) - descuento |
| descuento | Monto total de descuento aplicado | NUMERIC(10,2) | oro_promotion_applied_discount | amount | Agregado por line item desde descuentos aplicados |
| impuesto | Monto de impuestos aplicados (IVA 13%) | NUMERIC(10,2) | — | — | Calculado: subtotal * 0.13 |
| envio | Costo de envío | NUMERIC(10,2) | — | — | Por defecto 0.0 |
| total | Monto final total | NUMERIC(10,2) | — | — | Calculado: subtotal + impuesto + envio |
| costo_unitario | Costo estándar por unidad | NUMERIC(10,2) | dim_producto | costo_estandar | Obtenido desde dim_producto |
| costo_total | Costo total de la venta | NUMERIC(10,2) | — | — | Calculado: costo_unitario * cantidad |
| margen | Margen de ganancia | NUMERIC(10,2) | — | — | Calculado: subtotal - costo_total |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |
| impuesto_id | Tipo de impuesto aplicado | INT | dim_impuestos | impuesto_id | FK a dim_impuestos (usualmente IVA) |
| sk_promocion | Surrogate key de promoción | INT | dim_promocion | sk_promocion | FK a dim_promocion (1 por defecto = sin promoción) |
| line_item_id_externo | ID externo del line item | BIGINT | oro_order_line_item | id | ID original del line item en OroCommerce |
| orden_id_externo | ID externo de la orden | BIGINT | oro_order | id | ID original de la orden en OroCommerce |

---

## 2. fact_inventario

**Tipo de tabla:** Hechos  
**Nombre:** fact_inventario  
**Nombre visual:** Inventario  
**Descripción:** Registra los movimientos de inventario de productos en diferentes almacenes. Incluye entradas, salidas, transferencias y ajustes de stock.  
**Valores nulos:** No se permiten nulos en claves foráneas. Campos de observaciones pueden ser vacíos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| movimiento_id | ID único del movimiento (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| fecha_id | Fecha del movimiento de inventario | INT | movimientos_inventario.csv | fecha | Transformado a formato YYYYMMDD |
| producto_id | Producto involucrado en el movimiento | INT | movimientos_inventario.csv | id_producto | FK a dim_producto |
| almacen_id | Almacén donde ocurrió el movimiento | INT | movimientos_inventario.csv | id_almacen | FK a dim_almacen |
| tipo_movimiento_id | Tipo de movimiento realizado | INT | movimientos_inventario.csv | tipo_movimiento | FK a dim_tipo_movimiento |
| proveedor_id | Proveedor asociado (si aplica) | INT | movimientos_inventario.csv | id_proveedor | FK a dim_proveedor |
| usuario_id | Usuario que registró el movimiento | INT | dim_usuario | usuario_id | FK a dim_usuario |
| cantidad | Cantidad movida | NUMERIC(10,2) | movimientos_inventario.csv | cantidad | Puede ser positivo (entrada) o negativo (salida) |
| costo_unitario | Costo unitario del producto | NUMERIC(10,2) | movimientos_inventario.csv | costo_unitario | Costo promedio o último costo de compra |
| costo_total | Costo total del movimiento | NUMERIC(10,2) | movimientos_inventario.csv | costo_total | Calculado: cantidad * costo_unitario |
| stock_anterior | Stock antes del movimiento | NUMERIC(10,2) | movimientos_inventario.csv | stock_anterior | Stock existente antes de aplicar el movimiento |
| stock_resultante | Stock después del movimiento | NUMERIC(10,2) | movimientos_inventario.csv | stock_resultante | Stock resultante después de aplicar el movimiento |
| documento | Número de documento de referencia | VARCHAR(100) | movimientos_inventario.csv | documento | Factura, guía de remisión, nota de entrada, etc. |
| observaciones | Comentarios o notas adicionales | TEXT | movimientos_inventario.csv | observaciones | Notas sobre el movimiento (vacío si no hay) |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 3. fact_transacciones

**Tipo de tabla:** Hechos  
**Nombre:** fact_transacciones  
**Nombre visual:** Transacciones Contables  
**Descripción:** Registra todas las transacciones contables generadas desde las ventas. Cada venta genera múltiples asientos: ingreso, IVA, costo de ventas.  
**Valores nulos:** No se permiten nulos en claves foráneas obligatorias.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| transaccion_id | ID único de la transacción (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| fecha_id | Fecha de la transacción | INT | oro_order | created_at | Transformado a formato YYYYMMDD |
| cuenta_id | Cuenta contable afectada | INT | dim_cuenta_contable | cuenta_id | FK a dim_cuenta_contable |
| centro_costo_id | Centro de costo asociado | INT | dim_centro_costo | centro_costo_id | FK a dim_centro_costo |
| tipo_transaccion_id | Tipo de transacción | INT | dim_tipo_transaccion | tipo_transaccion_id | FK a dim_tipo_transaccion |
| usuario_id | Usuario que registró | INT | oro_order | user_owner_id | FK a dim_usuario |
| numero_asiento | Número del asiento contable | VARCHAR(50) | — | — | Generado secuencialmente: AST-000001, AST-000002, etc. |
| tipo_movimiento | Tipo de movimiento contable | VARCHAR(10) | — | — | DEBITO o CREDITO |
| monto | Monto de la transacción | NUMERIC(15,2) | oro_order | subtotal_value / total_value | Según tipo de asiento |
| documento_referencia | Documento de referencia | VARCHAR(100) | oro_order | id | Formato: ORD-{orden_id} |
| descripcion | Descripción del asiento | TEXT | — | — | Descripción generada según tipo |
| orden_id | Orden relacionada | INT | oro_order | id | FK a la orden que generó la transacción |
| movimiento_inventario_id | Movimiento de inventario relacionado | INT | fact_inventario | movimiento_id | FK al movimiento de inventario asociado (si aplica) |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |
| periodo_id | Período contable (YYYYMM) | INT | oro_order | created_at | Derivado: año*100 + mes de la orden |

**Nota:** Cada orden de venta genera 6 asientos contables: débito a bancos, crédito a ventas, débito/crédito a IVA, débito a costo de ventas, crédito a inventario.

---

## 4. fact_promocion_aplicada

**Tipo de tabla:** Hechos  
**Nombre:** fact_promocion_aplicada  
**Nombre visual:** Promociones Aplicadas  
**Descripción:** Registra las promociones aplicadas a órdenes específicas. Permite analizar el uso y efectividad de promociones y descuentos.  
**Valores nulos:** Campos opcionales pueden ser nulos si no hay promoción aplicada.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| sk_promocion_aplicada | Surrogate key (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| id_promocion_aplicada | ID de aplicación original | INT | oro_promotion_applied | id | ID de la aplicación en OroCommerce |
| sk_orden | Surrogate key de orden | INT | dim_orden | orden_id | FK al surrogate key de la orden |
| id_orden | ID externo de la orden | INT | oro_order | id | ID original de la orden en OroCommerce |
| orden_numero | Número de orden | VARCHAR(100) | oro_order | identifier | Identificador público de la orden |
| sk_promocion | Surrogate key de promoción | INT | dim_promocion | sk_promocion | FK al surrogate key de la promoción |
| nombre_promocion | Nombre de la promoción | VARCHAR(500) | oro_promotion | serialized_data->>'nombre' | Nombre de la promoción aplicada |
| tipo | Tipo de promoción | VARCHAR(50) | oro_promotion | serialized_data->>'codigo' | Tipo o código de la promoción |
| activa | Promoción activa | BOOLEAN | oro_promotion | — | TRUE si la promoción está vigente |
| total_orden | Total de la orden | NUMERIC(19,4) | oro_order | total_value | Monto total de la orden |
| descuentos_orden | Descuentos aplicados | NUMERIC(19,4) | oro_order | total_discounts_amount | Monto total de descuentos |
| fecha_aplicacion | Fecha de aplicación | TIMESTAMP | oro_promotion_applied | created_at | Fecha cuando se aplicó la promoción |
| fecha_carga | Fecha de carga en DW | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 5. fact_balance

**Tipo de tabla:** Hechos  
**Nombre:** fact_balance  
**Nombre visual:** Balance General  
**Descripción:** Contiene los saldos de cuentas contables por período, permitiendo generar el Balance General.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| balance_id | ID único del balance (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| periodo_id | Período contable (YYYYMM) | INT | fact_transacciones | periodo_id | Período contable asociado |
| cuenta_id | Cuenta contable | INT | fact_transacciones | cuenta_id | FK a dim_cuenta_contable |
| saldo_inicial | Saldo al inicio del período | NUMERIC(15,2) | — | — | Calculado desde transacciones del período anterior |
| debitos | Total de débitos del período | NUMERIC(15,2) | fact_transacciones | monto | Suma de todos los movimientos DEBITO |
| creditos | Total de créditos del período | NUMERIC(15,2) | fact_transacciones | monto | Suma de todos los movimientos CREDITO |
| saldo_final | Saldo al final del período | NUMERIC(15,2) | — | — | Calculado: saldo_inicial + debitos - creditos |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |
| fecha_id | Fecha del balance | INT | fact_transacciones | fecha_id | Derivado del período de transacciones |

---

## 6. fact_estado_resultados

**Tipo de tabla:** Hechos  
**Nombre:** fact_estado_resultados  
**Nombre visual:** Estado de Resultados  
**Descripción:** Contiene los datos para generar el Estado de Resultados (P&L). Muestra ingresos, costos, gastos y utilidades por período contable.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| resultado_id | ID único del registro (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| periodo_id | Período contable (YYYYMM) | INT | fact_transacciones | periodo_id | Período contable asociado |
| cuenta_id | Cuenta de ingresos/gastos | INT | fact_transacciones | cuenta_id | FK a dim_cuenta_contable |
| centro_costo_id | Centro de costo | INT | fact_transacciones | centro_costo_id | FK a dim_centro_costo |
| ingresos | Total de ingresos del período | NUMERIC(15,2) | fact_transacciones | monto | Suma de cuentas 4xxx (ventas e ingresos) |
| costos | Total de costos del período | NUMERIC(15,2) | fact_transacciones | monto | Suma de cuentas 5xxx (costo de ventas) |
| gastos | Total de gastos del período | NUMERIC(15,2) | fact_transacciones | monto | Suma de cuentas 6xxx (gastos operativos) |
| utilidad_bruta | Utilidad bruta del período | NUMERIC(15,2) | — | — | Calculado: ingresos - costos |
| utilidad_neta | Utilidad neta del período | NUMERIC(15,2) | — | — | Calculado: utilidad_bruta - gastos |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |
| fecha_id | Fecha del período | INT | fact_transacciones | fecha_id | Primer día del período (YYYYMM01) |

---

# TABLAS DE DIMENSIONES

## 7. dim_fecha

**Tipo de tabla:** Dimensión  
**Nombre:** dim_fecha  
**Nombre visual:** Fecha  
**Descripción:** Dimensión conformada de fechas con atributos de calendario. Rango: 2020-01-01 a 2030-12-31.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| fecha_id | Clave primaria (YYYYMMDD) | SERIAL | — | — | Generado automáticamente, aunque representa YYYYMMDD |
| fecha | Fecha completa | DATE | — | — | Fecha del día específico |
| anio | Año | INT | fecha | year | Extraído de la fecha |
| mes | Mes (1-12) | INT | fecha | month | Extraído de la fecha |
| dia | Día del mes (1-31) | INT | fecha | day | Extraído de la fecha |
| trimestre | Trimestre del año (1-4) | INT | fecha | quarter | Calculado: (mes - 1) // 3 + 1 |
| semana_anio | Semana del año (1-53) | INT | fecha | isocalendar().week | Número de semana ISO |
| dia_semana | Día de la semana (1-7) | INT | fecha | dayofweek | 1=Lunes, 7=Domingo |
| dia_semana_nombre | Nombre del día | VARCHAR(20) | — | — | Lunes, Martes, Miércoles, etc. |
| mes_nombre | Nombre del mes | VARCHAR(20) | — | — | Enero, Febrero, Marzo, etc. |
| es_fin_semana | Indica si es fin de semana | BOOLEAN | — | — | TRUE si día_semana IN (6,7) |
| es_festivo | Indica si es día festivo | BOOLEAN | — | — | Por defecto FALSE |
| nombre_festivo | Nombre del festivo | VARCHAR(100) | — | — | Vacío si no es festivo |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 8. dim_usuario

**Tipo de tabla:** Dimensión  
**Nombre:** dim_usuario  
**Nombre visual:** Usuario  
**Descripción:** Usuarios internos del sistema OroCommerce. Solo usuarios activos.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| usuario_id | ID único del usuario (PK) | SERIAL | oro_user | id | Surrogate key generado automáticamente |
| usuario_externo_id | ID externo del usuario | INT | oro_user | id | ID original de OroCommerce |
| username | Nombre de usuario | VARCHAR(255) | oro_user | username | Login del usuario |
| email | Email del usuario | VARCHAR(255) | oro_user | email | Email corporativo |
| nombre_completo | Nombre completo | VARCHAR(255) | oro_user | first_name, last_name | Concatenación: first_name + ' ' + last_name |
| activo | Usuario activo | BOOLEAN | oro_user | enabled | Solo se cargan usuarios enabled=TRUE |
| created_at | Fecha de creación | TIMESTAMP | oro_user | createdat | Fecha de registro en OroCommerce |
| updated_at | Fecha de actualización | TIMESTAMP | oro_user | createdat | Por defecto igual a created_at |

---

## 9. dim_producto

**Tipo de tabla:** Dimensión  
**Nombre:** dim_producto  
**Nombre visual:** Producto  
**Descripción:** Catálogo de productos de Punta Fina. Incluye precios de venta y costos de compra.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| producto_id | ID único del producto (PK) | SERIAL | oro_product | id | Surrogate key generado automáticamente |
| producto_externo_id | ID externo del producto | INT | oro_product | id | ID original de OroCommerce |
| sku | Código SKU del producto | VARCHAR(100) | oro_product | sku | Código único de inventario |
| nombre | Nombre del producto | VARCHAR(500) | oro_product | name | Nombre comercial del producto |
| descripcion | Descripción del producto | TEXT | oro_product | name | Por defecto igual al nombre |
| categoria | Categoría del producto | VARCHAR(100) | — | — | Por defecto "Calzado" |
| marca | Marca del producto | VARCHAR(100) | oro_product | name | Extraído del primer token del nombre |
| tipo | Tipo de producto | VARCHAR(50) | oro_product | type | simple, configurable, etc. |
| unidad_medida | Unidad de medida | VARCHAR(20) | — | — | Por defecto "Pieza" |
| precio_base | Precio de venta base | NUMERIC(10,2) | oro_price_product | value | Promedio de precios en oro_price_product |
| costo_estandar | Costo estándar de compra | NUMERIC(10,2) | Compras_Productos_PuntaFina.csv | Costo_Promedio_USD | Promedio de costos de compras |
| activo | Producto activo | BOOLEAN | oro_product | status | TRUE si status='enabled' |
| created_at | Fecha de creación | TIMESTAMP | oro_product | created_at | Fecha de registro en OroCommerce |

---

## 10. dim_cliente

**Tipo de tabla:** Dimensión  
**Nombre:** dim_cliente  
**Nombre visual:** Cliente  
**Descripción:** Clientes corporativos de Punta Fina.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| cliente_id | ID único del cliente (PK) | SERIAL | oro_customer | id | Surrogate key generado automáticamente |
| cliente_externo_id | ID externo del cliente | INT | oro_customer | id | ID original de OroCommerce |
| codigo_cliente | Código del cliente | VARCHAR(50) | — | — | Generado: CLI-{cliente_id con 6 dígitos} |
| nombre | Nombre del cliente | VARCHAR(255) | oro_customer | name | Nombre corporativo del cliente |
| tipo_cliente | Tipo de cliente | VARCHAR(50) | — | — | Por defecto "B2B" |
| segmento | Segmento del cliente | VARCHAR(50) | — | — | Por defecto "Regular" |
| email | Email de contacto | VARCHAR(255) | oro_customer_user | email | Email del primer usuario asociado |
| telefono | Teléfono de contacto | VARCHAR(50) | — | — | Por defecto "N/A" |
| activo | Cliente activo | BOOLEAN | — | — | Por defecto TRUE |
| fecha_registro | Fecha de registro | TIMESTAMP | oro_customer | created_at | Fecha de creación en OroCommerce |
| created_at | Fecha de carga en DW | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 11. dim_orden

**Tipo de tabla:** Dimensión  
**Nombre:** dim_orden  
**Nombre visual:** Orden  
**Descripción:** Órdenes de compra completas.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| orden_id | ID único de la orden (PK) | SERIAL | oro_order | id | Surrogate key generado automáticamente |
| orden_externo_id | ID externo de la orden | INT | oro_order | id | ID original de OroCommerce |
| numero_orden | Número de orden | VARCHAR(100) | oro_order | identifier | Identificador público de la orden |
| tipo_orden | Tipo de orden | VARCHAR(50) | — | — | Categorización de la orden |
| canal | Canal de venta | VARCHAR(50) | oro_order | channel | Canal por el que se realizó |
| moneda | Moneda de la orden | VARCHAR(3) | oro_order | currency | Código de moneda (USD, etc.) |
| tasa_cambio | Tasa de cambio aplicada | NUMERIC(10,4) | — | — | Tasa de cambio si aplica (default 1) |
| created_at | Fecha de creación en DW | TIMESTAMP | oro_order | created_at | Fecha de registro |

---

## 12. dim_promocion

**Tipo de tabla:** Dimensión  
**Nombre:** dim_promocion  
**Nombre visual:** Promoción  
**Descripción:** Promociones y campañas de marketing desde OroCommerce.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| sk_promocion | Surrogate key (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| id_promocion_source | ID de promoción en origen | INT | oro_promotion | id | ID original en OroCommerce (-1 para "Sin Promoción") |
| nombre_promocion | Nombre de la promoción | VARCHAR(500) | oro_promotion | serialized_data->>'nombre' | Nombre de la campaña |
| tipo_promocion | Tipo de promoción | VARCHAR(50) | oro_promotion | serialized_data->>'codigo' | Código/tipo de la promoción |
| usa_cupones | Usa cupones | BOOLEAN | oro_promotion | use_coupons | TRUE si requiere cupón |
| activa | Promoción activa | BOOLEAN | — | — | TRUE si está vigente |
| fecha_creacion | Fecha de creación | TIMESTAMP | oro_promotion | created_at | Fecha de registro en OroCommerce |
| fecha_actualizacion | Fecha de actualización | TIMESTAMP | oro_promotion | updated_at | Última modificación |
| fecha_carga | Fecha de carga en DW | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 13. dim_impuestos

**Tipo de tabla:** Dimensión  
**Nombre:** dim_impuestos  
**Nombre visual:** Impuestos  
**Descripción:** Tipos de impuestos aplicables (IVA, ISR, EXENTO).  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| impuesto_id | ID único del impuesto (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del impuesto | VARCHAR(50) | — | — | IVA, ISR, EXENTO |
| nombre | Nombre del impuesto | VARCHAR(100) | — | — | IVA 13%, ISR, Exento |
| tasa | Tasa del impuesto | NUMERIC(5,2) | — | — | 0.13 para IVA, 0.25 para ISR, 0.00 para EXENTO |
| tipo | Tipo de impuesto | VARCHAR(50) | — | — | ventas, renta, exento |

---

## 14. dim_almacen

**Tipo de tabla:** Dimensión  
**Nombre:** dim_almacen  
**Nombre visual:** Almacén  
**Descripción:** Almacenes y bodegas de Punta Fina desde CSV.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| almacen_id | ID único del almacén (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del almacén | VARCHAR(50) | almacenes.csv | id_almacen | Código único del almacén |
| nombre | Nombre del almacén | VARCHAR(255) | almacenes.csv | nombre_almacen | Nombre comercial |
| direccion | Dirección del almacén | VARCHAR(255) | almacenes.csv | direccion | Dirección física completa |
| ciudad | Ciudad | VARCHAR(100) | almacenes.csv | ciudad | Ciudad donde se ubica |
| pais | País | VARCHAR(100) | — | — | Por defecto "El Salvador" |
| capacidad | Capacidad en m³ | INT | almacenes.csv | capacidad_m3 | Capacidad total en metros cúbicos |
| tipo | Tipo de almacén | VARCHAR(50) | almacenes.csv | tipo_almacen | Principal, Secundario, Tránsito, etc. |
| activo | Almacén activo | BOOLEAN | almacenes.csv | activo | TRUE si está operativo |

---

## 15. dim_proveedor

**Tipo de tabla:** Dimensión  
**Nombre:** dim_proveedor  
**Nombre visual:** Proveedor  
**Descripción:** Proveedores de productos desde CSV.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| proveedor_id | ID único del proveedor (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del proveedor | VARCHAR(50) | proveedores.csv | id_proveedor | Código único (PROV001, PROV002, etc.) |
| nombre | Nombre del proveedor | VARCHAR(255) | proveedores.csv | nombre_proveedor | Razón social |
| contacto | Persona de contacto | VARCHAR(255) | proveedores.csv | contacto_principal | Nombre del contacto principal |
| email | Email de contacto | VARCHAR(255) | proveedores.csv | email | Email corporativo |
| telefono | Teléfono | VARCHAR(50) | proveedores.csv | telefono | Teléfono principal |
| direccion | Dirección | VARCHAR(255) | proveedores.csv | direccion | Dirección física completa |
| ciudad | Ciudad | VARCHAR(100) | proveedores.csv | ciudad | Ciudad donde se ubica |
| pais | País de origen | VARCHAR(100) | proveedores.csv | pais_origen | País del proveedor |
| activo | Proveedor activo | BOOLEAN | proveedores.csv | activo | TRUE si tiene relación comercial vigente |

---

## 16. dim_tipo_movimiento

**Tipo de tabla:** Dimensión  
**Nombre:** dim_tipo_movimiento  
**Nombre visual:** Tipo de Movimiento  
**Descripción:** Tipos de movimientos de inventario desde CSV.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| tipo_movimiento_id | ID único del tipo (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del tipo | VARCHAR(50) | tipos_movimiento.csv | id_tipo_movimiento | Código único (ENTRADA, SALIDA, etc.) |
| nombre | Nombre del tipo | VARCHAR(100) | tipos_movimiento.csv | nombre_tipo | Nombre descriptivo |
| descripcion | Descripción del tipo | TEXT | tipos_movimiento.csv | descripcion | Descripción detallada |
| tipo | Categoría del movimiento | VARCHAR(50) | tipos_movimiento.csv | tipo | entrada, salida, ajuste, etc. |
| afecta_stock | Afecta el stock | VARCHAR(20) | tipos_movimiento.csv | afecta_stock | positivo, negativo, neutro |
| activo | Tipo activo | BOOLEAN | tipos_movimiento.csv | activo | TRUE si está en uso |

---

## 17. dim_cuenta_contable

**Tipo de tabla:** Dimensión  
**Nombre:** dim_cuenta_contable  
**Nombre visual:** Cuenta Contable  
**Descripción:** Plan de cuentas contables desde CSV con jerarquía.  
**Valores nulos:** Cuenta padre puede ser vacío para cuentas de nivel 1.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| cuenta_id | ID único de la cuenta (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código de la cuenta | VARCHAR(50) | cuentas_contables.csv | id_cuenta | Código contable (1101, 4101, etc.) |
| nombre | Nombre de la cuenta | VARCHAR(255) | cuentas_contables.csv | nombre_cuenta | Nombre descriptivo |
| descripcion | Descripción | TEXT | cuentas_contables.csv | descripcion | Descripción detallada |
| tipo | Tipo de cuenta | VARCHAR(50) | cuentas_contables.csv | naturaleza | Activo, Pasivo, Patrimonio, Ingreso, Gasto |
| categoria | Categoría contable | VARCHAR(50) | cuentas_contables.csv | clasificacion | Clasificación específica |
| nivel | Nivel jerárquico | INT | cuentas_contables.csv | nivel | Nivel en el árbol contable (1, 2, 3, etc.) |
| cuenta_padre | Código de cuenta padre | VARCHAR(50) | cuentas_contables.csv | cuenta_padre | Código de la cuenta superior (vacío si nivel 1) |
| activo | Cuenta activa | BOOLEAN | cuentas_contables.csv | activa | TRUE si se usa actualmente |

---

## 18. dim_centro_costo

**Tipo de tabla:** Dimensión  
**Nombre:** dim_centro_costo  
**Nombre visual:** Centro de Costo  
**Descripción:** Centros de costo para asignación presupuestaria desde CSV.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| centro_costo_id | ID único del centro (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del centro | VARCHAR(50) | centros_costo.csv | id_centro_costo | Código único (CC001, CC002, etc.) |
| nombre | Nombre del centro | VARCHAR(255) | centros_costo.csv | nombre_centro | Nombre del área o departamento |
| descripcion | Descripción | TEXT | centros_costo.csv | nombre_centro | Descripción detallada |
| tipo | Tipo de centro | VARCHAR(50) | centros_costo.csv | tipo_centro | Operativo, Administrativo, Comercial, etc. |
| responsable | Responsable | VARCHAR(255) | centros_costo.csv | responsable | Nombre del responsable del centro |
| activo | Centro activo | BOOLEAN | centros_costo.csv | activo | TRUE si está operativo |

---

## 19. dim_tipo_transaccion

**Tipo de tabla:** Dimensión  
**Nombre:** dim_tipo_transaccion  
**Nombre visual:** Tipo de Transacción  
**Descripción:** Tipos de transacciones contables desde CSV.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| tipo_transaccion_id | ID único del tipo (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del tipo | VARCHAR(50) | tipos_transaccion.csv | id_tipo_transaccion | Código único (VENTA, COMPRA, etc.) |
| nombre | Nombre del tipo | VARCHAR(100) | tipos_transaccion.csv | nombre_tipo | Nombre descriptivo |
| descripcion | Descripción | TEXT | tipos_transaccion.csv | descripcion | Descripción detallada |
| categoria | Categoría | VARCHAR(50) | tipos_transaccion.csv | categoria | ingreso, gasto, transferencia, etc. |
| afecta_flujo | Afecta flujo de caja | VARCHAR(20) | — | — | positivo, negativo, neutro |
| activo | Tipo activo | BOOLEAN | — | — | TRUE si está en uso |

---

## 20. dim_periodo

**Tipo de tabla:** Dimensión  
**Nombre:** dim_periodo  
**Nombre visual:** Período Contable  
**Descripción:** Períodos contables mensuales. Generado automáticamente para rango 2020-2026.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| periodo_id | ID único del período (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del período | VARCHAR(10) | — | — | Formato: YYYYMM (202401, 202402, etc.) |
| anio | Año | INT | — | — | Año del período (2020-2026) |
| mes | Mes (1-12) | INT | — | — | Mes del período |
| trimestre | Trimestre (1-4) | INT | — | — | Calculado: (mes - 1) // 3 + 1 |
| nombre | Nombre del período | VARCHAR(100) | — | — | Formato: YYYY-MM (2024-01) |
| fecha_inicio | Fecha de inicio | DATE | — | — | Primer día del mes |
| fecha_fin | Fecha de fin | DATE | — | — | Último día del mes |
| activo | Período activo | BOOLEAN | — | — | TRUE si está abierto para transacciones |
| created_at | Fecha de creación | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## Estadísticas de Fuentes de Datos

### Tablas Origen OroCommerce
**Total de tablas utilizadas: 9**

| # | Tabla | Descripción | Uso principal |
|---|-------|-------------|---------------|
| 1 | oro_order | Órdenes de compra | fact_ventas, fact_transacciones, dim_orden |
| 2 | oro_order_line_item | Líneas de detalle de órdenes | fact_ventas (detalle de productos vendidos) |
| 3 | oro_product | Catálogo de productos | dim_producto (productos y precios base) |
| 4 | oro_price_product | Precios de productos | dim_producto (precios de venta) |
| 5 | oro_customer | Clientes corporativos | dim_cliente |
| 6 | oro_customer_user | Usuarios de clientes | dim_cliente (contactos) |
| 7 | oro_user | Usuarios internos | dim_usuario |
| 8 | oro_promotion | Promociones y campañas | dim_promocion |
| 9 | oro_promotion_applied | Aplicaciones de promociones | fact_promocion_aplicada |
| 10 | oro_promotion_applied_discount | Descuentos aplicados | fact_ventas (descuentos por línea) |

**Nota:** Todas las tablas de OroCommerce se acceden a través de conexión PostgreSQL directa al schema `public` de la base de datos `orocommerce`.

### Tablas Origen OroCRM
**Total de tablas utilizadas: 1**

| # | Tabla | Descripción | Uso principal |
|---|-------|-------------|---------------|
| 1 | orocrm_channel | Canales de venta | dim_orden (canales de venta) |

**Nota:** La tabla de OroCRM se accede a través de conexión PostgreSQL directa al schema `public` de la base de datos `orocrm`.

### Archivos CSV Utilizados
**Total de archivos CSV: 11**

#### Inventario (5 archivos)
| # | Archivo | Ruta | Uso | Registros aprox. |
|---|---------|------|-----|------------------|
| 1 | almacenes.csv | data/inputs/inventario/ | dim_almacen | ~10 |
| 2 | proveedores.csv | data/inputs/inventario/ | dim_proveedor | ~50 |
| 3 | tipos_movimiento.csv | data/inputs/inventario/ | dim_tipo_movimiento | ~10 |
| 4 | movimientos_inventario.csv | data/inputs/inventario/ | fact_inventario | ~5,000+ |
| 5 | Compras_Productos_PuntaFina.csv | raíz del proyecto | dim_producto (costos estándar) | ~2,000 |

#### Finanzas (4 archivos)
| # | Archivo | Ruta | Uso | Registros aprox. |
|---|---------|------|-----|------------------|
| 6 | cuentas_contables.csv | data/inputs/finanzas/ | dim_cuenta_contable | ~100 |
| 7 | centros_costo.csv | data/inputs/finanzas/ | dim_centro_costo | ~15 |
| 8 | tipos_transaccion.csv | data/inputs/finanzas/ | dim_tipo_transaccion | ~20 |
| 9 | transacciones_contables.csv | data/inputs/finanzas/ | fact_transacciones (opcional) | Variable |

#### Ventas (2 archivos)
| # | Archivo | Ruta | Uso | Registros aprox. |
|---|---------|------|-----|------------------|
| 10 | estados_orden.csv | data/inputs/ventas/ | Catálogo de estados (no cargado actualmente) | ~10 |
| 11 | metodos_envio.csv | data/inputs/ventas/ | Catálogo de métodos (no cargado actualmente) | ~5 |

#### Dimensión Especial
| # | Archivo | Ruta | Uso | Registros aprox. |
|---|---------|------|-----|------------------|
| 12 | dim_fechas.csv | data/inputs/ | dim_fecha (alternativa) | ~3,650 |

**Nota:** Los archivos CSV se procesan con pandas `read_csv()` utilizando codificación UTF-8.

### Resumen de Fuentes

```
┌─────────────────────────────────────────┐
│   FUENTES DE DATOS - DATA WAREHOUSE     │
├─────────────────────────────────────────┤
│ OroCommerce:     10 tablas              │
│ OroCRM:           1 tabla               │
│ CSV Inventario:   5 archivos            │
│ CSV Finanzas:     4 archivos            │
│ CSV Ventas:       2 archivos            │
│ CSV Otros:        1 archivo             │
├─────────────────────────────────────────┤
│ TOTAL:           23 fuentes de datos    │
└─────────────────────────────────────────┘
```

### Distribución de Tablas DW por Fuente

| Fuente | Dimensiones | Hechos | Total |
|--------|-------------|--------|-------|
| OroCommerce | 6 (cliente, usuario, producto, orden, promocion, impuestos) | 2 (ventas, promocion_aplicada) | 8 |
| OroCRM | 0 | 0 | 0 |
| CSV Inventario | 3 (almacen, proveedor, tipo_movimiento) | 1 (inventario) | 4 |
| CSV Finanzas | 4 (cuenta_contable, centro_costo, tipo_transaccion, periodo) | 3 (transacciones, balance, estado_resultados) | 7 |
| CSV + Generado | 1 (fecha) | 0 | 1 |
| **TOTAL DW** | **14** | **6** | **20** |

---

## Notas Generales

### Convenciones de Nombres
- **Tablas de Hechos:** Prefijo `fact_`
- **Tablas de Dimensiones:** Prefijo `dim_`
- **Primary Keys:** SERIAL (generado automáticamente)
- **Foreign Keys:** Mismo nombre que la tabla referenciada + `_id`

### Tipos de Datos Estándar
- **IDs:** SERIAL (auto-increment) o INT
- **Montos:** NUMERIC(15,2) o NUMERIC(10,2)
- **Textos Cortos:** VARCHAR(50/100/255)
- **Textos Largos:** TEXT
- **Fechas:** DATE (YYYY-MM-DD)
- **Timestamps:** TIMESTAMP
- **Booleanos:** BOOLEAN

### Reglas de Carga ETL
1. **Valores Nulos:** Se reemplazan con valores por defecto (0, "", FALSE, etc.)
2. **Claves Foráneas:** Si no existe referencia, se asigna ID=1 (registro "Sin Información")
3. **Fechas:** Formato estándar ISO 8601 (YYYY-MM-DD)
4. **Timestamps:** Con timezone
5. **IDs Externos:** Mantienen el ID original del sistema fuente para trazabilidad

### Sistemas Fuente
- **OroCommerce:** Datos de productos, clientes, órdenes, line items, promociones
- **OroCRM:** Datos de canales y relaciones con clientes
- **CSVs:** Datos de inventario, finanzas, proveedores, almacenes
- **Calculados:** Métricas derivadas (márgenes, totales, utilidades)

---

**Documento generado:** 16 de enero de 2026  
**Versión:** 2.0  
**Basado en:** Estructura real de datawarehouse_bi  
**Autor:** GitHub Copilot  
**Proyecto:** Data Warehouse Punta Fina
