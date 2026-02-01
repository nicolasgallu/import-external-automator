# Meli & Google Drive Automation Job

Este proyecto es un servicio de automatización diseñado para integrarse con la API de **Mercado Libre** y **Google Drive**. Su función principal es monitorear el estado de las publicaciones, identificar infracciones de moderación y gestionar automáticamente estructuras de carpetas en la nube para el almacenamiento de activos de productos.

## 🚀 Funcionalidades Principales

* **Sincronización de Estados (Meli):** * Recupera de forma asíncrona todos los ítems de un usuario.
* Identifica ítems con estados no activos (`paused`, `under_review`, etc.).
* Consulta el endpoint de **moderaciones** para extraer la razón específica (`reason`) y la solución propuesta (`remedy`) de cada infracción.


* **Automatización de Google Drive:** * Genera automáticamente carpetas de respaldo/organización para cada `item_id`.
* Implementa control de concurrencia mediante semáforos para evitar límites de cuota de la API.
* Retorna las URLs directas de las carpetas creadas para su persistencia en base de datos.


* **Gestión de Secretos y Seguridad:** * Integración nativa con **GCP Secret Manager** para el manejo de tokens de acceso.
* Soporte para **Application Default Credentials (ADC)**, permitiendo una transición transparente entre entornos locales y Google Cloud Platform (Cloud Run/Jobs).



## 🛠️ Stack Técnico

* **Python 3.10+**
* **Asyncio & Aiohttp:** Para el procesamiento masivo y concurrente de peticiones API.
* **Google Cloud SDK:** Secret Manager, Auth y Drive API.
* **SQLAlchemy:** Para la carga masiva (Bulk Load) de resultados en MySQL.

---

## ⚙️ Configuración y Requisitos

Para que el proyecto funcione correctamente, se deben configurar los siguientes componentes:

### 1. Variables de Entorno (`.env`)

El proyecto utiliza un archivo de configuración que mapea las siguientes variables:

| Variable | Descripción |
| --- | --- |
| `PROJECT_ID` | ID del proyecto en Google Cloud Console. |
| `SECRET_ID` | Nombre del secreto en Secret Manager que contiene el token de Meli. |
| `PARENT_FOLDER_ID` | ID de la carpeta de Google Drive donde se crearán las subcarpetas. |
| `SCOPES` | Scopes de Google API (ej: `https://www.googleapis.com/auth/drive`). |
| `MAX_CONCURRENT_TASKS` | Límite de tareas asíncronas simultáneas (ej: `20`). |
| `INSTANCE_DB`, `USER_DB`, etc. | Credenciales de conexión para la base de datos MySQL. |

### 2. Google Cloud Platform (GCP)

* **Habilitar APIs:** Google Drive API, Secret Manager API y Cloud SQL Admin API.
* **Service Account:** La identidad que ejecute el código (o el Job en Cloud Run) debe tener:
* Permisos de **Secret Manager Secret Accessor**.
* Permiso de **Editor** compartido en la carpeta de Google Drive definida en `PARENT_FOLDER_ID`.



### 3. Mercado Libre

* Es necesario que el secreto en GCP contenga un JSON con la estructura: `{"questions": {"TOKEN": "tu_access_token"}}`.

---

## 📈 Flujo de Ejecución

1. **Auth:** Se obtienen las credenciales ADC y el token de Mercado Libre desde Secret Manager.
2. **Meli Scan:** Se listan los productos y se filtran aquellos que requieren atención (moderaciones).
3. **Drive Sync:** Se crean las carpetas faltantes en Google Drive de forma concurrente.
4. **Database Update:** Se consolidan los estados y URLs de Drive para realizar un `UPDATE` masivo en la base de datos local o Cloud SQL.

---
