const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const sql = require('mssql');
const log4js = require('log4js');
const SftpClient = require('ssh2-sftp-client');

// Configuración Logs
log4js.configure(path.resolve(__dirname, './config/log4js.json'));

const logger = log4js.getLogger("ProcessCSVToTable");

// Configuración SQL Server
const sqlConfig = require(path.resolve(__dirname, './config/mssql.js'));
const dbConn = new sql.ConnectionPool(sqlConfig.databaseSippy);

// Configuración SFTP
const sftpConfig = require(path.resolve(__dirname, './config/sftpConfig.js'));

// Ruta del archivo .csv en el servidor SFTP
let csvFilePath = '' ///  Data/Call_Records_from_CCS_2023-04-01_00_00_00_to_2023-05-01_00_00_00.csv;
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


// Función para obtener el nombre del archivo CSV automáticamente desde el servidor SFTP
async function getCSVFileNameFromSFTP() {
    const sftp = new SftpClient();

    return new Promise((resolve, reject) => {
        sftp.connect(sftpConfig)
            .then(() => {
                return sftp.list(sftpConfig.remotePath); // Obtener la lista de archivos en la ruta remota
            })
            .then((files) => {
                // Busco por tipo de archivo  considerando que siempre será .csv
                const csvFile = files.find(file => file.name.endsWith('.csv'));

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
                logger.error('Error al descargar el archivo CSV:', error + ' Remote path' + remoteFilePath);
                reject(error);
            });
    });
}



// Función principal para leer el archivo .csv y procesar los datos
async function processCSVFile(csvFilePath) {
    const data = [];

    fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
            //Me falta agregar las columnas  finales , pero con esto puedo ir avanzando
            const rowData = {};

            // Verificar el valor de 'Billing Prefix'
            if (row['Billing Prefix'] === 'onnet_in') {
                rowData.columna3 = row.CLD; // Insertar 'CLD' si cumple la condición
                rowData.columna4 = row.CLI
            } else {
                rowData.columna3 = row.CLI; // Insertar 'CLI' si no cumple la condición
                rowData.columna4 = row.CLD
            }

            // Agregar las demás columnas al objeto JSON
            rowData.columna1 = row.Caller;
            rowData.columna2 = row.Country;
            rowData.columna5 = row['Duration, sec'];
            rowData.columna6 = row['Connect Time'];
            rowData.columna7 = row['Disconnect Time'];
            rowData.columna8 = JSON.stringify(row);

            data.push(rowData);
        })
        .on('end', async () => {
            await connectToDatabase();
            await insertDataToTable(data);
            await dbConn.close();
        });
}


async function insertDataToTable(data) {
    try {
        const transaction = dbConn.transaction();
        await transaction.begin();

        const request = transaction.request();

        // Crear la tabla temporal global
        await request.query(`
            CREATE TABLE ##TempCDR (
                Trunk VARCHAR(255),
                Country VARCHAR(255),
                DID VARCHAR(255),
                Phone VARCHAR(255),
                Duration INT,
                ConnectTime DATETIME,
                DisconnectTime DATETIME,
                Data NVARCHAR(4000)
            );
        `);

        // Llenar la tabla temporal global dentro de la transacción
        for (const row of data) {
            await transaction.request().query(`
                INSERT INTO ##TempCDR (Trunk, Country, DID, Phone, Duration, ConnectTime, DisconnectTime, Data)
                VALUES ('${row.columna1}', '${row.columna2}', '${row.columna3}', '${row.columna4}', ${row.columna5}, '${row.columna6}', '${row.columna7}', '${row.columna8}');
            `);
        }

        // Merge con la tabla principal
        await transaction.request().query(`
            MERGE INTO CDR AS target
USING (
    SELECT DISTINCT DID, Phone, ConnectTime, Trunk, Country, Duration, DisconnectTime, Data
    FROM ##TempCDR
) AS source
ON target.DID = source.DID AND target.Phone = source.Phone AND target.ConnectTime = source.ConnectTime 
WHEN MATCHED THEN
    UPDATE SET
    target.Trunk = source.Trunk,
    target.Country = source.Country,
    target.Duration = source.Duration,
    target.DisconnectTime = source.DisconnectTime,
    target.Data = source.Data
WHEN NOT MATCHED THEN
    INSERT (Trunk, Country, DID, Phone, Duration, ConnectTime, DisconnectTime, Data)
    VALUES (source.Trunk, source.Country, source.DID, source.Phone, source.Duration, source.ConnectTime, source.DisconnectTime, source.Data);
        `);

        // Eliminar la tabla temporal global (opcional, ya que se eliminará automáticamente al finalizar la sesión)
        // await transaction.request().query(`DROP TABLE ##TempCDR;`);

        await transaction.commit();
        logger.info('Datos insertados o actualizados correctamente.');
    } catch (error) {
        logger.error('Error al insertar o actualizar los datos:', error);
    }
}




// Función para mover el archivo CSV a otra carpeta en el servidor SFTP
async function moveCSVToProcessedFolderOnSFTP(csvFilePath) {
    const sftp = new SftpClient();

    return new Promise((resolve, reject) => {
        sftp.connect(sftpConfig)
            .then(() => {
                let remoteProcessedFolderPath = sftpConfig.remotePath + 'procesado'; // Ruta de la carpeta de destino en el servidor SFTP
                let remoteProcessedFilePath = path.join(remoteProcessedFolderPath, path.basename(csvFilePath));

                //Como estoy en window hago un replace  del las diagonales
                // remoteProcessedFilePath = path.posix.normalize(remoteProcessedFilePath); // remoteProcessedFilePath.replace('/', '\\')
                //remoteProcessedFolderPath = path.posix.normalize(remoteProcessedFolderPath); //  remoteProcessedFolderPath.replace('/', '\\')

                return sftp.mkdir(remoteProcessedFolderPath, true) // Crea la carpeta de destino (si no existe) 
                    .then(() => sftp.rcopy(csvFilePath, remoteProcessedFilePath)) // Copia el archivo al destino
                    .then(() => sftp.delete(csvFilePath)); // Elimina el archivo original
            })
            .then(() => {
                sftp.end();
                logger.info(`Archivo CSV movido a la carpeta de procesados en el servidor SFTP.`);
                resolve();
            })
            .catch((error) => {
                sftp.end();
                logger.error('Error al mover el archivo CSV en el servidor SFTP:', error);
                reject(error);
            });
    });
}

// ...

// Ejecuta el proceso de obtención del nombre del archivo CSV  , Insertar datos y por ultimo mover el archivo a procesado
getCSVFileNameFromSFTP()
    .then((fileName) => {
        csvFilePath = sftpConfig.remotePath + `${fileName}`;

        // LocalFilePath = path.resolve(__dirname, 'Data', fileName);
        return downloadCSVFromSFTP(csvFilePath);
    })
    .then((localFilePath) => {
        processCSVFile(localFilePath);
        return moveCSVToProcessedFolderOnSFTP(csvFilePath);
    })
    .then(() => {
        logger.info('Proceso completado. El archivo CSV ha sido movido a la carpeta de procesados en el servidor SFTP.');
    })
    .catch((error) => {
        logger.error('Error durante el procesamiento:', error);
    });




