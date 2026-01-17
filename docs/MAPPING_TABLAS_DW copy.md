# MAPPING DE TABLAS - DATA WAREHOUSE PUNTA FINA

## Índice

### Tablas de Hechos (Facts)
1. [fact_ventas](#1-fact_ventas)
2. [fact_inventario](#2-fact_inventario)
3. [fact_transacciones](#3-fact_transacciones)
4. [fact_balance](#4-fact_balance)
5. [fact_estado_resultados](#5-fact_estado_resultados)

### Tablas de Dimensiones (Dimensions)
6. [dim_fecha](#6-dim_fecha)
7. [dim_usuario](#7-dim_usuario)
8. [dim_producto](#8-dim_producto)
9. [dim_cliente](#9-dim_cliente)
10. [dim_sitio_web](#10-dim_sitio_web)
11. [dim_canal](#11-dim_canal)
12. [dim_direccion](#12-dim_direccion)
13. [dim_orden](#13-dim_orden)
14. [dim_line_item](#14-dim_line_item)
15. [dim_detalle_venta](#15-dim_detalle_venta)
16. [dim_envio](#16-dim_envio)
17. [dim_estado_orden](#17-dim_estado_orden)
18. [dim_estado_pago](#18-dim_estado_pago)
19. [dim_pago](#19-dim_pago)
20. [dim_impuestos](#20-dim_impuestos)
21. [dim_promocion](#21-dim_promocion)
22. [dim_almacen](#22-dim_almacen)
23. [dim_proveedor](#23-dim_proveedor)
24. [dim_tipo_movimiento](#24-dim_tipo_movimiento)
25. [dim_cuenta_contable](#25-dim_cuenta_contable)
26. [dim_centro_costo](#26-dim_centro_costo)
27. [dim_tipo_transaccion](#27-dim_tipo_transaccion)
28. [dim_periodo_contable](#28-dim_periodo_contable)

---

# TABLAS DE HECHOS

##! 1. fact_ventas

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
| cliente_id | Cliente que realizó la compra | INT | oro_order | customer_id | ID directo de OroCommerce (no requiere lookup) |
| producto_id | Producto vendido | INT | oro_order_line_item | product_id | ID directo de OroCommerce (no requiere lookup) |
| orden_id | Orden asociada al line item | INT | oro_order_line_item | order_id | ID directo de OroCommerce (no requiere lookup) |
| usuario_id | Usuario interno responsable | INT | oro_order | user_owner_id | Relación directa con dim_usuario (ID de OroCommerce) |
| line_item_id | Identificador del line item | INT | oro_order_line_item | id | ID del line item específico en la orden |
| promocion_id_externo | Promoción aplicada (si existe) | INT | oro_promotion_applied_discount | source_promotion_id | Se obtiene de la promoción principal aplicada al line item |
| cantidad | Número de unidades vendidas | NUMERIC(10,2) | oro_order_line_item | quantity | Directo desde la línea de la orden |
| precio_unitario | Precio por unidad | NUMERIC(10,2) | oro_order_line_item | value | Precio aplicado por unidad |
| subtotal_bruto | Subtotal antes de descuentos | NUMERIC(10,2) | — | — | Calculado: cantidad * precio_unitario |
| descuento_total | Monto total de descuento aplicado | NUMERIC(10,2) | oro_promotion_applied_discount | amount | Agregado por line item desde descuentos aplicados |
| subtotal | Subtotal después de descuentos | NUMERIC(10,2) | — | — | Calculado: subtotal_bruto - descuento_total |
| impuesto | Monto de impuestos aplicados (IVA 13%) | NUMERIC(10,2) | — | — | Calculado: subtotal * 0.13 |
| envio | Costo de envío | NUMERIC(10,2) | — | — | Por defecto 0.0 (se puede enriquecer desde oro_order_shipping_tracking) |
| total | Monto final total | NUMERIC(10,2) | — | — | Calculado: subtotal + impuesto + envio |
| costo_unitario | Costo estándar por unidad | NUMERIC(10,2) | dim_producto | costo_estandar | Obtenido desde dim_producto (viene de CSV de compras) |
| costo_total | Costo total de la venta | NUMERIC(10,2) | — | — | Calculado: costo_unitario * cantidad |
| margen | Margen de ganancia | NUMERIC(10,2) | — | — | Calculado: subtotal - costo_total |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

##! 2. fact_inventario

**Tipo de tabla:** Hechos  
**Nombre:** fact_inventario  
**Nombre visual:** Inventario  
**Descripción:** Registra los movimientos de inventario de productos en diferentes almacenes. Incluye entradas, salidas, transferencias y ajustes de stock. Permite rastrear el flujo de productos y calcular niveles de inventario históricos.  
**Valores nulos:** No se permiten nulos en claves foráneas. Campos de observaciones pueden ser vacíos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| movimiento_id | ID único del movimiento (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| fecha_id | Fecha del movimiento de inventario | INT | movimientos_inventario.csv | fecha | Transformado a formato YYYYMMDD y cruzado con dim_fecha |
| producto_id | Producto involucrado en el movimiento | INT | movimientos_inventario.csv | id_producto | Código de producto del CSV, cruzado con dim_producto |
| almacen_id | Almacén donde ocurrió el movimiento | INT | movimientos_inventario.csv | id_almacen | Código de almacén del CSV, cruzado con dim_almacen |
| tipo_movimiento_id | Tipo de movimiento realizado | INT | movimientos_inventario.csv | tipo_movimiento | Código del tipo (entrada, salida, etc.), cruzado con dim_tipo_movimiento |
| proveedor_id | Proveedor asociado (si aplica) | INT | movimientos_inventario.csv | id_proveedor | Código de proveedor del CSV, cruzado con dim_proveedor |
| usuario_id | Usuario que registró el movimiento | INT | dim_usuario | usuario_id | Se asigna el primer usuario disponible en dim_usuario |
| cantidad | Cantidad movida | NUMERIC(10,2) | movimientos_inventario.csv | cantidad | Puede ser positivo (entrada) o negativo (salida) |
| costo_unitario | Costo unitario del producto | NUMERIC(10,2) | movimientos_inventario.csv | costo_unitario | Costo promedio o último costo de compra |
| costo_total | Costo total del movimiento | NUMERIC(10,2) | movimientos_inventario.csv | costo_total | Calculado: cantidad * costo_unitario |
| stock_anterior | Stock antes del movimiento | NUMERIC(10,2) | movimientos_inventario.csv | stock_anterior | Stock existente antes de aplicar el movimiento |
| stock_resultante | Stock después del movimiento | NUMERIC(10,2) | movimientos_inventario.csv | stock_resultante | Stock resultante después de aplicar el movimiento |
| documento | Número de documento de referencia | VARCHAR(50) | movimientos_inventario.csv | documento | Factura, guía de remisión, nota de entrada, etc. |
| observaciones | Comentarios o notas adicionales | TEXT | movimientos_inventario.csv | observaciones | Notas sobre el movimiento (vacío si no hay) |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

##! 3. fact_transacciones

**Tipo de tabla:** Hechos  
**Nombre:** fact_transacciones  
**Nombre visual:** Transacciones Contables  
**Descripción:** Registra todas las transacciones contables generadas desde las ventas. Cada venta genera múltiples asientos: ingreso, IVA, costo de ventas. Permite análisis financiero y contable detallado.  
**Valores nulos:** No se permiten nulos en claves foráneas obligatorias. Campos de referencia pueden ser vacíos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| transaccion_id | ID único de la transacción (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| fecha_id | Fecha de la transacción | INT | oro_order | created_at | Transformado a formato YYYYMMDD desde la fecha de la orden |
| periodo_id | Período contable (YYYYMM) | INT | oro_order | created_at | Derivado: año*100 + mes de la orden |
| cuenta_id | Cuenta contable afectada | INT | dim_cuenta_contable | cuenta_id | Mapeo según tipo de asiento (ventas, bancos, IVA, costo) |
| centro_costo_id | Centro de costo asociado | INT | dim_centro_costo | centro_costo_id | Centro de costo predeterminado (primer registro disponible) |
| tipo_transaccion_id | Tipo de transacción | INT | dim_tipo_transaccion | tipo_transaccion_id | Se busca tipo "VENTA" o primer tipo disponible |
| usuario_id | Usuario que registró | INT | oro_order | user_owner_id | Usuario propietario de la orden en OroCommerce |
| numero_asiento | Número del asiento contable | VARCHAR(50) | — | — | Generado secuencialmente: AST-000001, AST-000002, etc. |
| tipo_movimiento | Tipo de movimiento contable | VARCHAR(20) | — | — | DEBITO o CREDITO según la naturaleza del asiento |
| monto | Monto de la transacción | NUMERIC(12,2) | oro_order | subtotal_value / total_value | Según tipo de asiento: total, subtotal, IVA o costo |
| documento_referencia | Documento de referencia | VARCHAR(100) | oro_order | id | Formato: ORD-{orden_id} |
| descripcion | Descripción del asiento | TEXT | — | — | Descripción generada según tipo: "Cobro orden #X", "Ingreso por venta", etc. |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

**Nota:** Cada orden de venta genera 6 asientos contables:
1. Débito a Bancos (entrada efectivo)
2. Crédito a Ventas (reconocimiento ingreso)
3. Débito a IVA (impuesto cobrado)
4. Crédito a IVA por Pagar (obligación fiscal)
5. Débito a Costo de Ventas (reconocimiento costo)
6. Crédito a Inventario (salida de mercancía)

---

##! 4. fact_balance

**Tipo de tabla:** Hechos  
**Nombre:** fact_balance  
**Nombre visual:** Balance General  
**Descripción:** Contiene los saldos de cuentas contables por período, permitiendo generar el Balance General. Muestra activos, pasivos y patrimonio en momentos específicos del tiempo.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| balance_id | ID único del balance (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| fecha_id | Fecha del balance | INT | fact_transacciones | fecha_id | Derivado del período de transacciones |
| periodo_id | Período contable (YYYYMM) | INT | fact_transacciones | periodo_id | Período contable asociado |
| cuenta_id | Cuenta contable | INT | fact_transacciones | cuenta_id | Cuenta del balance (activo, pasivo, patrimonio) |
| centro_costo_id | Centro de costo | INT | fact_transacciones | centro_costo_id | Centro de costo asociado |
| saldo_inicial | Saldo al inicio del período | NUMERIC(12,2) | — | — | Calculado desde transacciones del período anterior |
| debitos | Total de débitos del período | NUMERIC(12,2) | fact_transacciones | monto | Suma de todos los movimientos DEBITO |
| creditos | Total de créditos del período | NUMERIC(12,2) | fact_transacciones | monto | Suma de todos los movimientos CREDITO |
| saldo_final | Saldo al final del período | NUMERIC(12,2) | — | — | Calculado: saldo_inicial + debitos - creditos |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

##! 5. fact_estado_resultados

**Tipo de tabla:** Hechos  
**Nombre:** fact_estado_resultados  
**Nombre visual:** Estado de Resultados  
**Descripción:** Contiene los datos para generar el Estado de Resultados (P&L). Muestra ingresos, costos, gastos y utilidades por período contable. Permite análisis de rentabilidad.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| resultado_id | ID único del registro (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| fecha_id | Fecha del período | INT | fact_transacciones | fecha_id | Primer día del período (YYYYMM01) |
| periodo_id | Período contable (YYYYMM) | INT | fact_transacciones | periodo_id | Período contable asociado |
| cuenta_id | Cuenta de ingresos/gastos | INT | fact_transacciones | cuenta_id | Cuenta de resultados (4xxx, 5xxx, 6xxx) |
| centro_costo_id | Centro de costo | INT | fact_transacciones | centro_costo_id | Centro de costo asociado |
| ingresos | Total de ingresos del período | NUMERIC(12,2) | fact_transacciones | monto | Suma de cuentas 4xxx (ventas e ingresos) |
| costos | Total de costos del período | NUMERIC(12,2) | fact_transacciones | monto | Suma de cuentas 5xxx (costo de ventas) |
| gastos | Total de gastos del período | NUMERIC(12,2) | fact_transacciones | monto | Suma de cuentas 6xxx (gastos operativos) |
| utilidad_bruta | Utilidad bruta del período | NUMERIC(12,2) | — | — | Calculado: ingresos - costos |
| utilidad_neta | Utilidad neta del período | NUMERIC(12,2) | — | — | Calculado: utilidad_bruta - gastos |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

# TABLAS DE DIMENSIONES

##!dim6. dim_fecha

**Tipo de tabla:** Dimensión  
**Nombre:** dim_fecha  
**Nombre visual:** Fecha  
**Descripción:** Dimensión conformada de fechas con atributos de calendario. Permite análisis temporal por día, semana, mes, trimestre y año. Rango: 2020-01-01 a 2030-12-31.  
**Valores nulos:** No se permiten nulos. Todos los atributos son obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| fecha_id | Clave primaria (YYYYMMDD) | INT | — | — | Generado: formato YYYYMMDD de la fecha |
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
| es_festivo | Indica si es día festivo | BOOLEAN | — | — | Por defecto FALSE (puede enriquecerse) |
| nombre_festivo | Nombre del festivo | VARCHAR(100) | — | — | Vacío si no es festivo |
| created_at | Fecha de carga del registro | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

##! 7. dim_usuario

**Tipo de tabla:** Dimensión  
**Nombre:** dim_usuario  
**Nombre visual:** Usuario  
**Descripción:** Usuarios internos del sistema OroCommerce. Vendedores, administradores y personal autorizado. Solo usuarios activos.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| usuario_id | ID único del usuario (PK) | INT | oro_user | id | ID directo de OroCommerce |
| usuario_externo_id | ID externo del usuario | INT | oro_user | id | Mismo que usuario_id (para trazabilidad) |
| username | Nombre de usuario | VARCHAR(100) | oro_user | username | Login del usuario |
| email | Email del usuario | VARCHAR(255) | oro_user | email | Email corporativo |
| nombre_completo | Nombre completo | VARCHAR(255) | oro_user | first_name, last_name | Concatenación: first_name + ' ' + last_name |
| activo | Usuario activo | BOOLEAN | oro_user | enabled | Solo se cargan usuarios enabled=TRUE |
| created_at | Fecha de creación | TIMESTAMP | oro_user | createdat | Fecha de registro en OroCommerce |
| updated_at | Fecha de actualización | TIMESTAMP | oro_user | createdat | Por defecto igual a created_at |

---

## 8. dim_producto ------------------------------!

**Tipo de tabla:** Dimensión  
**Nombre:** dim_producto  
**Nombre visual:** Producto  
**Descripción:** Catálogo de productos de Punta Fina. Incluye información básica, precios de venta y costos de compra. Los precios vienen de oro_price_product y los costos del CSV de compras.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| producto_id | ID único del producto (PK) | INT | oro_product | id | ID directo de OroCommerce |
| producto_externo_id | ID externo del producto | INT | oro_product | id | Mismo que producto_id (para trazabilidad) |
| sku | Código SKU del producto | VARCHAR(255) | oro_product | sku | Código único de inventario |
| nombre | Nombre del producto | VARCHAR(255) | oro_product | name | Nombre comercial del producto |
| descripcion | Descripción del producto | TEXT | oro_product | name | Por defecto igual al nombre |
| categoria | Categoría del producto | VARCHAR(100) | — | — | Por defecto "Calzado" (puede enriquecerse) |
| marca | Marca del producto | VARCHAR(100) | oro_product | name | Extraído del primer token del nombre |
| tipo | Tipo de producto | VARCHAR(50) | oro_product | type | Tipo según OroCommerce (simple, configurable, etc.) |
| unidad_medida | Unidad de medida | VARCHAR(20) | — | — | Por defecto "Pieza" |
| precio_base | Precio de venta base | NUMERIC(10,2) | oro_price_product | value | Promedio de precios en oro_price_product |
| costo_estandar | Costo estándar de compra | NUMERIC(10,2) | Compras_Productos_PuntaFina.csv | Costo_Promedio_USD | Promedio de costos de compras |
| activo | Producto activo | BOOLEAN | oro_product | status | TRUE si status='enabled' |
| created_at | Fecha de creación | TIMESTAMP | oro_product | created_at | Fecha de registro en OroCommerce |

**Nota:** Si un producto no tiene precio, se estima como costo_estandar * 2.5. Si no tiene costo, se estima como precio_base * 0.4.

---

## 9. dim_cliente ------------------------

**Tipo de tabla:** Dimensión  
**Nombre:** dim_cliente  
**Nombre visual:** Cliente  
**Descripción:** Clientes corporativos de Punta Fina. Información básica de contacto y clasificación. El email se obtiene desde oro_customer_user.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| cliente_id | ID único del cliente (PK) | INT | oro_customer | id | ID directo de OroCommerce |
| cliente_externo_id | ID externo del cliente | INT | oro_customer | id | Mismo que cliente_id (para trazabilidad) |
| codigo_cliente | Código del cliente | VARCHAR(50) | — | — | Generado: CLI-{cliente_id con 6 dígitos} |
| nombre | Nombre del cliente | VARCHAR(255) | oro_customer | name | Nombre corporativo del cliente |
| tipo_cliente | Tipo de cliente | VARCHAR(50) | — | — | Por defecto "B2B" (todos son corporativos) |
| segmento | Segmento del cliente | VARCHAR(50) | — | — | Por defecto "Regular" (puede enriquecerse) |
| email | Email de contacto | VARCHAR(255) | oro_customer_user | email | Email del primer usuario asociado |
| telefono | Teléfono de contacto | VARCHAR(50) | — | — | Por defecto "N/A" (no disponible en origen) |
| activo | Cliente activo | BOOLEAN | — | — | Por defecto TRUE |
| fecha_registro | Fecha de registro | TIMESTAMP | oro_customer | created_at | Fecha de creación en OroCommerce |
| created_at | Fecha de carga en DW | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 10. dim_sitio_web ----------------

**Tipo de tabla:** Dimensión  
**Nombre:** dim_sitio_web  
**Nombre visual:** Sitio Web  
**Descripción:** Sitios web configurados en OroCommerce. Permite análisis por tienda online.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| sitio_web_id | ID único del sitio web (PK) | INT | oro_website | id | ID directo de OroCommerce |
| nombre | Nombre del sitio | VARCHAR(255) | oro_website | name | Nombre configurado del sitio |
| url | URL del sitio | VARCHAR(255) | oro_website | url | URL base del sitio web |
| activo | Sitio activo | BOOLEAN | — | — | Por defecto TRUE |
| created_at | Fecha de creación | TIMESTAMP | oro_website | created_at | Fecha de registro en OroCommerce |

---

## 11. dim_canal

**Tipo de tabla:** Dimensión  
**Nombre:** dim_canal  
**Nombre visual:** Canal  
**Descripción:** Canales de venta configurados en OroCRM. Permite análisis por canal de marketing/ventas.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| canal_id | ID único del canal (PK) | INT | orocrm_channel | id | ID directo de OroCRM |
| nombre | Nombre del canal | VARCHAR(255) | orocrm_channel | name | Nombre configurado del canal |
| tipo_canal | Tipo de canal | VARCHAR(100) | orocrm_channel | channel_type | Tipo según OroCRM (web, retail, wholesale, etc.) |
| activo | Canal activo | BOOLEAN | orocrm_channel | status | TRUE si status='active' |
| created_at | Fecha de creación | TIMESTAMP | orocrm_channel | created_at | Fecha de registro en OroCRM |

---

## 12. dim_direccion

**Tipo de tabla:** Dimensión  
**Nombre:** dim_direccion  
**Nombre visual:** Dirección  
**Descripción:** Direcciones de clientes y pedidos. Información de envío y facturación.  
**Valores nulos:** Campos opcionales pueden ser vacíos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| direccion_id | ID único de la dirección (PK) | INT | oro_order_address | id | ID directo de OroCommerce |
| direccion_externo_id | ID externo de la dirección | INT | oro_order_address | id | Mismo que direccion_id (para trazabilidad) |
| direccion_completa | Dirección completa | TEXT | oro_order_address | street, street2 | Concatenación de street + street2 |
| ciudad | Ciudad | VARCHAR(100) | oro_order_address | city | Ciudad de la dirección |
| region | Región o estado | VARCHAR(100) | oro_order_address | region_text | Región/estado textual |
| codigo_postal | Código postal | VARCHAR(20) | oro_order_address | postal_code | Código postal |
| pais | País | VARCHAR(100) | oro_order_address | country_code | Código de país (iso2) |
| tipo_direccion | Tipo de dirección | VARCHAR(50) | — | — | "Envío" o "Facturación" |
| created_at | Fecha de creación | TIMESTAMP | oro_order_address | created | Fecha de registro en OroCommerce |

---

## 13. dim_orden

**Tipo de tabla:** Dimensión  
**Nombre:** dim_orden  
**Nombre visual:** Orden  
**Descripción:** Órdenes de compra completas. Cabecera de las órdenes con información agregada.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| orden_id | ID único de la orden (PK) | INT | oro_order | id | ID directo de OroCommerce |
| orden_externo_id | ID externo de la orden | INT | oro_order | id | Mismo que orden_id (para trazabilidad) |
| numero_orden | Número de orden | VARCHAR(100) | oro_order | identifier | Identificador público de la orden |
| cliente_id | Cliente que realizó la orden | INT | oro_order | customer_id | FK a dim_cliente |
| usuario_id | Usuario propietario | INT | oro_order | user_owner_id | FK a dim_usuario |
| fecha_orden | Fecha de la orden | DATE | oro_order | created_at | Fecha de creación |
| estado_orden | Estado de la orden | VARCHAR(50) | oro_order_workflow_step | name | Estado actual del workflow |
| subtotal | Subtotal de la orden | NUMERIC(12,2) | oro_order | subtotal_value | Suma de line items |
| descuento_total | Descuento total | NUMERIC(12,2) | oro_order | total_discounts_amount | Suma de descuentos |
| total | Total de la orden | NUMERIC(12,2) | oro_order | total_value | Total a pagar |
| moneda | Moneda de la orden | VARCHAR(10) | oro_order | currency | Código de moneda (USD, etc.) |
| created_at | Fecha de creación en DW | TIMESTAMP | oro_order | created_at | Fecha de registro |

---

## 14. dim_line_item

**Tipo de tabla:** Dimensión  
**Nombre:** dim_line_item  
**Nombre visual:** Línea de Pedido  
**Descripción:** Líneas individuales de pedido. Detalle de cada producto en una orden.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| line_item_id | ID único del line item (PK) | INT | oro_order_line_item | id | ID directo de OroCommerce |
| line_item_externo_id | ID externo del line item | INT | oro_order_line_item | id | Mismo que line_item_id (para trazabilidad) |
| orden_id | Orden asociada | INT | oro_order_line_item | order_id | FK a dim_orden |
| producto_id | Producto vendido | INT | oro_order_line_item | product_id | FK a dim_producto |
| sku | SKU del producto | VARCHAR(255) | oro_order_line_item | product_sku | Código del producto al momento de la orden |
| nombre_producto | Nombre del producto | VARCHAR(255) | oro_order_line_item | product_name | Nombre del producto al momento de la orden |
| cantidad | Cantidad solicitada | NUMERIC(10,2) | oro_order_line_item | quantity | Unidades pedidas |
| precio_unitario | Precio por unidad | NUMERIC(10,2) | oro_order_line_item | value | Precio aplicado |
| subtotal | Subtotal del line item | NUMERIC(10,2) | — | — | Calculado: cantidad * precio_unitario |
| created_at | Fecha de creación | TIMESTAMP | oro_order_line_item | created_at | Fecha de registro |

---

## 15. dim_detalle_venta

**Tipo de tabla:** Dimensión  
**Nombre:** dim_detalle_venta  
**Nombre visual:** Detalle de Venta  
**Descripción:** Detalles adicionales de ventas. Información complementaria de las transacciones.  
**Valores nulos:** Campos opcionales pueden ser vacíos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| detalle_venta_id | ID único del detalle (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| orden_id | Orden asociada | INT | oro_order | id | FK a dim_orden |
| line_item_id | Line item asociado | INT | oro_order_line_item | id | FK a dim_line_item |
| notas_venta | Notas de la venta | TEXT | oro_order | customer_notes | Comentarios del cliente |
| notas_internas | Notas internas | TEXT | oro_order | internal_notes | Comentarios internos del equipo |
| referencia_externa | Referencia externa | VARCHAR(255) | oro_order | po_number | Número de orden de compra del cliente |
| created_at | Fecha de carga | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 16. dim_envio

**Tipo de tabla:** Dimensión  
**Nombre:** dim_envio  
**Nombre visual:** Envío  
**Descripción:** Métodos y detalles de envío. Información logística de las órdenes.  
**Valores nulos:** Campos opcionales pueden ser vacíos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| envio_id | ID único del envío (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| metodo_envio | Método de envío | VARCHAR(100) | oro_shipping_method_config | method | Método configurado (standard, express, etc.) |
| transportista | Transportista | VARCHAR(100) | oro_shipping_method_config | type | Tipo/proveedor de envío |
| costo_envio | Costo del envío | NUMERIC(10,2) | oro_order_shipping_tracking | estimated_delivery_date | Costo estimado |
| numero_tracking | Número de rastreo | VARCHAR(255) | oro_order_shipping_tracking | number | Código de rastreo |
| fecha_envio | Fecha de envío | DATE | oro_order_shipping_tracking | created | Fecha de creación del tracking |
| fecha_entrega_estimada | Fecha estimada de entrega | DATE | oro_order_shipping_tracking | estimated_delivery_date | Fecha estimada |
| estado_envio | Estado del envío | VARCHAR(50) | — | — | En proceso, enviado, entregado, etc. |
| created_at | Fecha de carga | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 17. dim_estado_orden

**Tipo de tabla:** Dimensión  
**Nombre:** dim_estado_orden  
**Nombre visual:** Estado de Orden  
**Descripción:** Estados posibles de las órdenes en el workflow. Catálogo de estados desde CSV.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| estado_orden_id | ID único del estado (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo_estado | Código del estado | VARCHAR(50) | estados_orden.csv | id_estado_orden | Código único del estado |
| nombre_estado | Nombre del estado | VARCHAR(100) | estados_orden.csv | nombre_estado | Nombre descriptivo |
| descripcion | Descripción del estado | TEXT | estados_orden.csv | descripcion | Descripción detallada |
| es_final | Indica si es estado final | BOOLEAN | estados_orden.csv | es_final | TRUE si el flujo termina aquí |
| created_at | Fecha de carga | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 18. dim_estado_pago

**Tipo de tabla:** Dimensión  
**Nombre:** dim_estado_pago  
**Nombre visual:** Estado de Pago  
**Descripción:** Estados posibles de los pagos. Catálogo de estados desde CSV.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| estado_pago_id | ID único del estado (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo_estado | Código del estado | VARCHAR(50) | estados_pago.csv | id_estado_pago | Código único del estado |
| nombre_estado | Nombre del estado | VARCHAR(100) | estados_pago.csv | nombre_estado | Nombre descriptivo |
| descripcion | Descripción del estado | TEXT | estados_pago.csv | descripcion | Descripción detallada |
| permite_reintento | Permite reintentar pago | BOOLEAN | estados_pago.csv | permite_reintento | TRUE si se puede reintentar |
| created_at | Fecha de carga | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

---

## 19. dim_pago

**Tipo de tabla:** Dimensión  
**Nombre:** dim_pago  
**Nombre visual:** Método de Pago  
**Descripción:** Métodos de pago disponibles. Catálogo estático de formas de pago.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| pago_id | ID único del método (PK) | INT | — | — | ID secuencial estático (1-10) |
| pago_externo_id | ID externo del método | INT | — | — | Mismo que pago_id |
| metodo_pago | Método de pago | VARCHAR(100) | — | — | Efectivo, Tarjeta Crédito, Transferencia, etc. |
| procesador | Procesador del pago | VARCHAR(100) | — | — | Manual, Visa/MC, PayPal, Stripe, etc. |
| tipo_pago | Tipo de pago | VARCHAR(50) | — | — | Inmediato, Diferido, Crédito |

**Nota:** Dimensión estática con 10 métodos predefinidos.

---

## 20. dim_impuestos

**Tipo de tabla:** Dimensión  
**Nombre:** dim_impuestos  
**Nombre visual:** Impuestos  
**Descripción:** Tipos de impuestos aplicables. Catálogo estático de impuestos (IVA, ISR, EXENTO).  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| impuesto_id | ID único del impuesto (PK) | INT | — | — | ID secuencial estático (1-3) |
| codigo | Código del impuesto | VARCHAR(20) | — | — | IVA, ISR, EXENTO |
| nombre | Nombre del impuesto | VARCHAR(100) | — | — | IVA 13%, ISR, Exento |
| tasa | Tasa del impuesto | NUMERIC(5,4) | — | — | 0.13 para IVA, 0.25 para ISR, 0.0 para EXENTO |
| tipo | Tipo de impuesto | VARCHAR(50) | — | — | ventas, renta, exento |

**Nota:** Dimensión estática con 3 impuestos predefinidos.

---

## 21. dim_promocion

**Tipo de tabla:** Dimensión  
**Nombre:** dim_promocion  
**Nombre visual:** Promoción  
**Descripción:** Promociones y campañas de marketing desde OroCommerce. Incluye promociones con o sin cupones.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| sk_promocion | Surrogate key (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| id_promocion_source | ID de promoción en origen | INT | oro_promotion | id | ID original en OroCommerce (-1 para "Sin Promoción") |
| nombre_promocion | Nombre de la promoción | VARCHAR(255) | oro_promotion | serialized_data->>'nombre' | Nombre de la campaña |
| tipo_promocion | Tipo de promoción | VARCHAR(100) | oro_promotion | serialized_data->>'codigo' | Código/tipo de la promoción |
| usa_cupones | Usa cupones | BOOLEAN | oro_promotion | use_coupons | TRUE si requiere cupón |
| activa | Promoción activa | BOOLEAN | — | — | TRUE si está vigente |
| fecha_creacion | Fecha de creación | TIMESTAMP | oro_promotion | created_at | Fecha de registro en OroCommerce |
| fecha_actualizacion | Fecha de actualización | TIMESTAMP | oro_promotion | updated_at | Última modificación |
| fecha_carga | Fecha de carga en DW | TIMESTAMP | — | — | Timestamp del momento de la carga ETL |

**Nota:** Incluye un registro por defecto con id_promocion_source=-1 para "Sin Promoción".

---

## 22. dim_almacen

**Tipo de tabla:** Dimensión  
**Nombre:** dim_almacen  
**Nombre visual:** Almacén  
**Descripción:** Almacenes y bodegas de Punta Fina. Información de ubicación y capacidad desde CSV.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| almacen_id | ID único del almacén (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del almacén | VARCHAR(50) | almacenes.csv | id_almacen | Código único del almacén |
| nombre | Nombre del almacén | VARCHAR(255) | almacenes.csv | nombre_almacen | Nombre comercial |
| direccion | Dirección del almacén | TEXT | almacenes.csv | direccion | Dirección física completa |
| ciudad | Ciudad | VARCHAR(100) | almacenes.csv | ciudad | Ciudad donde se ubica |
| pais | País | VARCHAR(100) | — | — | Por defecto "El Salvador" |
| capacidad | Capacidad en m³ | INT | almacenes.csv | capacidad_m3 | Capacidad total en metros cúbicos |
| tipo | Tipo de almacén | VARCHAR(50) | almacenes.csv | tipo_almacen | Principal, Secundario, Tránsito, etc. |
| activo | Almacén activo | BOOLEAN | almacenes.csv | activo | TRUE si está operativo |

---

## 23. dim_proveedor

**Tipo de tabla:** Dimensión  
**Nombre:** dim_proveedor  
**Nombre visual:** Proveedor  
**Descripción:** Proveedores de productos de Punta Fina. Información de contacto y ubicación desde CSV.  
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
| direccion | Dirección | TEXT | proveedores.csv | direccion | Dirección física completa |
| ciudad | Ciudad | VARCHAR(100) | proveedores.csv | ciudad | Ciudad donde se ubica |
| pais | País de origen | VARCHAR(100) | proveedores.csv | pais_origen | País del proveedor |
| activo | Proveedor activo | BOOLEAN | proveedores.csv | activo | TRUE si tiene relación comercial vigente |

---

## 24. dim_tipo_movimiento

**Tipo de tabla:** Dimensión  
**Nombre:** dim_tipo_movimiento  
**Nombre visual:** Tipo de Movimiento  
**Descripción:** Tipos de movimientos de inventario. Catálogo de operaciones desde CSV.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| tipo_movimiento_id | ID único del tipo (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del tipo | VARCHAR(50) | tipos_movimiento.csv | id_tipo_movimiento | Código único (ENTRADA, SALIDA, etc.) |
| nombre | Nombre del tipo | VARCHAR(100) | tipos_movimiento.csv | nombre_tipo | Nombre descriptivo |
| afecta_stock | Afecta el stock | VARCHAR(20) | tipos_movimiento.csv | afecta_stock | positivo, negativo, neutro |
| requiere_autorizacion | Requiere autorización | BOOLEAN | tipos_movimiento.csv | requiere_autorizacion | TRUE si necesita aprobación |
| activo | Tipo activo | BOOLEAN | tipos_movimiento.csv | activo | TRUE si está en uso |

---

## 25. dim_cuenta_contable

**Tipo de tabla:** Dimensión  
**Nombre:** dim_cuenta_contable  
**Nombre visual:** Cuenta Contable  
**Descripción:** Plan de cuentas contables. Catálogo del plan contable desde CSV con jerarquía.  
**Valores nulos:** No se permiten nulos en campos obligatorios. Cuenta padre puede ser vacío para cuentas de nivel 1.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| cuenta_id | ID único de la cuenta (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código de la cuenta | VARCHAR(20) | cuentas_contables.csv | id_cuenta | Código contable (1101, 4101, etc.) |
| nombre | Nombre de la cuenta | VARCHAR(255) | cuentas_contables.csv | nombre_cuenta | Nombre descriptivo |
| descripcion | Descripción | TEXT | cuentas_contables.csv | descripcion | Descripción detallada |
| tipo | Tipo de cuenta | VARCHAR(50) | cuentas_contables.csv | naturaleza | Activo, Pasivo, Patrimonio, Ingreso, Gasto |
| categoria | Categoría contable | VARCHAR(100) | cuentas_contables.csv | clasificacion | Clasificación específica |
| nivel | Nivel jerárquico | INT | cuentas_contables.csv | nivel | Nivel en el árbol contable (1, 2, 3, etc.) |
| cuenta_padre | Código de cuenta padre | VARCHAR(20) | cuentas_contables.csv | cuenta_padre | Código de la cuenta superior (vacío si nivel 1) |
| activo | Cuenta activa | BOOLEAN | cuentas_contables.csv | activa | TRUE si se usa actualmente |

---

## 26. dim_centro_costo

**Tipo de tabla:** Dimensión  
**Nombre:** dim_centro_costo  
**Nombre visual:** Centro de Costo  
**Descripción:** Centros de costo para asignación presupuestaria. Desde CSV.  
**Valores nulos:** No se permiten nulos en campos obligatorios.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| centro_costo_id | ID único del centro (PK) | SERIAL | — | — | Generado automáticamente por PostgreSQL |
| codigo | Código del centro | VARCHAR(50) | centros_costo.csv | id_centro_costo | Código único (CC001, CC002, etc.) |
| nombre | Nombre del centro | VARCHAR(255) | centros_costo.csv | nombre_centro | Nombre del área o departamento |
| descripcion | Descripción | TEXT | centros_costo.csv | nombre_centro | Descripción detallada (por defecto igual al nombre) |
| tipo | Tipo de centro | VARCHAR(50) | centros_costo.csv | tipo_centro | Operativo, Administrativo, Comercial, etc. |
| responsable | Responsable | VARCHAR(255) | centros_costo.csv | responsable | Nombre del responsable del centro |
| activo | Centro activo | BOOLEAN | centros_costo.csv | activo | TRUE si está operativo |

---

## 27. dim_tipo_transaccion

**Tipo de tabla:** Dimensión  
**Nombre:** dim_tipo_transaccion  
**Nombre visual:** Tipo de Transacción  
**Descripción:** Tipos de transacciones contables. Catálogo desde CSV.  
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

## 28. dim_periodo_contable

**Tipo de tabla:** Dimensión  
**Nombre:** dim_periodo_contable  
**Nombre visual:** Período Contable  
**Descripción:** Períodos contables mensuales. Generado automáticamente para rango 2020-2026.  
**Valores nulos:** No se permiten nulos.

### Atributos

| Nombre del atributo | Descripción | Tipo de dato | Tabla origen | Columna origen | Origen del dato / Comentario |
|---------------------|-------------|--------------|--------------|----------------|------------------------------|
| periodo_id | ID del período (YYYYMM) | INT | — | — | Generado: año*100 + mes (202401, 202402, etc.) |
| anio | Año | INT | — | — | Año del período (2020-2026) |
| mes | Mes (1-12) | INT | — | — | Mes del período |
| trimestre | Trimestre (1-4) | INT | — | — | Calculado: (mes - 1) // 3 + 1 |
| nombre_periodo | Nombre del período | VARCHAR(50) | — | — | Formato: YYYY-MM (2024-01) |
| fecha_inicio | Fecha de inicio | DATE | — | — | Primer día del mes |
| fecha_fin | Fecha de fin | DATE | — | — | Último día del mes |
| cerrado | Período cerrado | BOOLEAN | — | — | Por defecto FALSE (puede cerrarse manualmente) |

---

## Notas Generales

### Convenciones de Nombres
- **Tables de Hechos:** Prefijo `fact_`
- **Tablas de Dimensiones:** Prefijo `dim_`
- **Primary Keys:** Sufijo `_id` (SERIAL para generado, INT para ID externo)
- **Foreign Keys:** Mismo nombre que la PK referenciada

### Tipos de Datos Estándar
- **IDs:** INT o SERIAL
- **Montos:** NUMERIC(12,2) o NUMERIC(10,2)
- **Textos Cortos:** VARCHAR(100) o VARCHAR(255)
- **Textos Largos:** TEXT
- **Fechas:** DATE (YYYY-MM-DD)
- **Timestamps:** TIMESTAMP
- **Booleanos:** BOOLEAN

### Reglas de Carga ETL
1. **Valores Nulos:** Se reemplazan con valores por defecto (0, "", FALSE, etc.)
2. **Claves Foráneas:** Si no existe referencia, se asigna ID=1 (registro "Sin Información")
3. **Fechas:** Formato estándar ISO 8601 (YYYY-MM-DD)
4. **Timestamps:** Zona horaria UTC
5. **IDs Externos:** Mantienen el ID original del sistema fuente para trazabilidad

### Sistemas Fuente
- **OroCommerce:** Datos de productos, clientes, órdenes, line items
- **OroCRM:** Datos de canales y relaciones con clientes
- **CSVs:** Datos de inventario, finanzas, proveedores, almacenes
- **Calculados:** Métricas derivadas (márgenes, totales, utilidades)

---

**Documento generado:** 16 de enero de 2026  
**Versión:** 1.0  
**Autor:** GitHub Copilot  
**Proyecto:** Data Warehouse Punta Fina
