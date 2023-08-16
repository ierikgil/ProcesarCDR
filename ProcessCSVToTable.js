const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const sql = require('mssql');
const log4js = require('log4js');
const SftpClient = require('ssh2-sftp-client');

// Configuraci�n Logs
log4js.configure(path.resolve(__dirname, './config/log4js.json'));

const logger = log4js.getLogger("ProcessCSVToTable");

// Configuraci�n SQL Server
const sqlConfig = require(path.resolve(__dirname, './config/mssql.js'));
const dbConn = new sql.ConnectionPool(sqlConfig.databaseSippy);

// Configuraci�n SFTP
const sftpConfig = require(path.resolve(__dirname, './config/sftpConfig.js'));

// Ruta del archivo .csv en el servidor SFTP
let csvFilePath = '' 
let localFilePath = ''
let remoteFilePath = ''

// Funci�n para establecer la conexi�n a la base de datos
async function connectToDatabase() {
    try {
        await dbConn.connect();
        logger.info('Conexi�n establecida correctamente.');
    } catch (error) {
        logger.error('Error al conectar a la base de datos:', error);
    }
}


// Funci�n para obtener el nombre del archivo CSV autom�ticamente desde el servidor SFTP
async function getCSVFileNameFromSFTP() {
    const sftp = new SftpClient();

    return new Promise((resolve, reject) => {
        sftp.connect(sftpConfig)
            .then(() => {
                return sftp.list(sftpConfig.remotePath); // Obtener la lista de archivos en la ruta remota
            })
            .then((files) => {
                // Busco por tipo de archivo  considerando que siempre ser� .csv
                const csvFile = files.find(file => file.name.endsWith('.csv'));

                if (csvFile) {
                    resolve(csvFile.name);
                } else {
                    reject(new Error('No se encontr� ning�n archivo CSV en el servidor SFTP.' + sftpConfig.remotePath));
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


// Funci�n para descargar el archivo CSV desde el servidor SFTP
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



// Funci�n principal para leer el archivo .csv y procesar los datos
async function processCSVFile(csvFilePath) {
    const data = [];

    fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
            //Me falta agregar las columnas  finales , pero con esto puedo ir avanzando
            const rowData = {};

            // Agregar todas las columnas al objeto JSON
            for (const column in row) {
                rowData[column] = row[column];
            }

            data.push({
                columna1: row.Caller,
                columna2: row.Country,
                columna3: row.CLI,
                columna4: row['Duration, sec'],
                columna5: row['Connect Time'],
                columna6: row['Disconnect Time'],
                columna7: JSON.stringify(rowData)
            });
        })
        .on('end', async () => {
            await connectToDatabase();
            await insertDataToTable(data);
            await dbConn.close();
        });
}


// Funci�n para insertar los datos en la tabla
async function insertDataToTable(data) {
    try {
        const transaction = dbConn.transaction();
        await transaction.begin();

        const request = transaction.request();
        for (const row of data) {
            await request.query(`
        INSERT INTO CDR (Trunk, Country, CLI, Duration, ConnectTime, DisconnectTime, Data )
        VALUES ('${row.columna1}', '${row.columna2}', '${row.columna3}', '${row.columna4}', '${row.columna5}', '${row.columna6}', '${row.columna7}' )
      `);
        }

        await transaction.commit();
        logger.info('Datos insertados correctamente.');
    } catch (error) {
        logger.error('Error al insertar los datos:', error);
    }
}

// Funci�n para mover el archivo CSV a otra carpeta en el servidor SFTP
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

// Ejecuta el proceso de obtenci�n del nombre del archivo CSV  , Insertar datos y por ultimo mover el archivo a procesado
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




            