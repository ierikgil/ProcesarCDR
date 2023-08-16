const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const sql = require('mssql');
const log4js = require('log4js');
const SftpClient = require('ssh2-sftp-client');

// Configuración Logs
log4js.configure(path.resolve(__dirname, './config/log4js.json'));
const logger = log4js.getLogger("ProcessCSV");

// Configuración SQL Server
const sqlConfig = require(path.resolve(__dirname, './config/mssql.js'));
const dbConn = new sql.ConnectionPool(sqlConfig.databaseSippy);

// Configuración SFTP
const sftpConfig = require(path.resolve(__dirname, './config/sftpConfig.js'));

// Ruta del archivo .csv en el servidor SFTP
let csvFilePath = '' ///Data/Call_Records_from_CCS_2023-04-01_00_00_00_to_2023-05-01_00_00_00.csv';
let localFilePath = ''
let remoteFilePath = ''

// Función para establecer la conexión a la base de datos
async function connectToDatabase() {
    try {
        await dbConn.connect();
        logger.info('Conexión establecida correctamente.');
    } catch (error) {
        logger.error('Error al conectar a la base de datos:', error);
    }
}

// Función para insertar los datos en la tabla
async function insertDataToTable(data) {
    try {
        const transaction = dbConn.transaction();
        await transaction.begin();

        const request = transaction.request();
        for (const row of data) {
            await request.query(`
        INSERT INTO CDR (Trunk, Country, CLI)
        VALUES ('${row.columna1}', '${row.columna2}', '${row.columna3}')
      `);
        }

        await transaction.commit();
        logger.info('Datos insertados correctamente.');
    } catch (error) {
        logger.error('Error al insertar los datos:', error);
    }
}

// Función principal para leer el archivo .csv y procesar los datos
async function processCSVFile(csvFilePath) {
    const data = [];

    fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
            // Agrega las columnas y los nombres de propiedad según corresponda al archivo .csv
            data.push({
                columna1: row.Caller,
                columna2: row['Original CLI'],
                columna3: row.CLI,
            });
        })
        .on('end', async () => {
            await connectToDatabase();
            await insertDataToTable(data);
            await dbConn.close();
        });
}



// Función para descargar el archivo CSV desde el servidor SFTP
async function downloadCSVFromSFTP() {
    const sftp = new SftpClient();

    return new Promise((resolve, reject) => {
        sftp.connect(sftpConfig)
            .then(() => {
                 localFilePath = path.join(__dirname, 'temp/Data/', path.basename(csvFilePath));
                 remoteFilePath = sftpConfig.remotePath + '/' + path.basename(csvFilePath);

                return sftp.get(remoteFilePath, localFilePath);
            })
            .then(() => {
                sftp.end();
                logger.info(`Archivo CSV descargado: ${localFilePath}`);
                resolve(localFilePath);
            })
            .catch((error) => {
                sftp.end();
                logger.error('Error al descargar el archivo CSV:', error + ' Remote path'+ remoteFilePath );
                reject(error);
            });
    });
}

/*
// Ejecuta el proceso de descarga y procesamiento del archivo CSV desde el servidor SFTP
downloadCSVFromSFTP()
    .then((localFilePath) => {
        processCSVFile(localFilePath);
    })
    .catch((error) => {
        logger.error('Error al descargar el archivo CSV:', error);
    });
    */


// ...

// Función para obtener el nombre del archivo CSV automáticamente desde el servidor SFTP
async function getCSVFileNameFromSFTP() {
    const sftp = new SftpClient();

    return new Promise((resolve, reject) => {
        sftp.connect(sftpConfig)
            .then(() => {
                return sftp.list(sftpConfig.remotePath); // Obtener la lista de archivos en la ruta remota
            })
            .then((files) => {
                // Filtrar y seleccionar el archivo deseado (puedes ajustar esto según tus criterios)
                const csvFile = files.find(file =>  file.name.endsWith('.csv'));

                if (csvFile) {
                    resolve(csvFile.name);
                } else {
                    reject(new Error('No se encontró ningún archivo CSV en el servidor SFTP.' + sftpConfig.remotePath));
                }
            })
            .catch((error) => {
                logger.error('Error al obtener el nombre del archivo CSV:', error);
                reject(error);
            })
            .finally(() => {
                sftp.end();
            });
    });
}

// ...

// Ejecuta el proceso de obtención del nombre del archivo CSV y su posterior descarga y procesamiento
getCSVFileNameFromSFTP()
    .then((fileName) => {
         csvFilePath = `./Data/${fileName}`;
        return downloadCSVFromSFTP(csvFilePath);
    })
    .then((localFilePath) => {
        processCSVFile(localFilePath);
    })
    .catch((error) => {
        logger.error('Error durante el procesamiento:', error + ' Ruta:'+csvFilePath);
    });
