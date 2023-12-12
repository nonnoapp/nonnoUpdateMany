// *Importações
const fs = require("fs");
const csv = require("csv-parser");
const { MongoClient } = require("mongodb");
const logFilePath = "log.txt";
const { ObjectId } = require("mongodb");

// *Constantes
//Importante: o arquivo csv deve estar separado por ";" e a primeira coluna deve conter os IDs
const csvFilePath = "./cuidadores_ativos.csv";
const mongoURL = "URL VINDA DO MONGODB";
const dbName = "nonnodb";
const collectionName = "caregivers";

// *Função que atualiza o status dos documentos no MongoDB
async function updateStatus(ids) {
  const client = new MongoClient(mongoURL);

  try {
    await client.connect();
    log("Conectado ao MongoDB");

    const db = client.db(dbName);
    log(`Conectado ao banco de dados ${dbName}`);
    const collection = db.collection(collectionName);
    log(`Conectado à coleção ${collectionName}`);
    log(`Atualizando status para ${ids.length} documentos`);
    log(`IDs: ${ids.join(", ")}`);
    const result = await collection.updateMany(
      { _id: { $in: ids } },
      { $set: { "steps.status": "valid", "steps.stage": "active" } }
    );
    log(`Foram encontrados ${result.matchedCount} documentos`);
    log(`Status atualizado para ${result.modifiedCount} documentos`);
    console.log(result);
  } finally {
    await client.close();
    log("Conexão fechada");
  }
}

//* Função para escrever no log
function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(logFilePath, logMessage, "utf-8");
  console.log(logMessage);
}

//* Função principal
async function main() {
  const ids = [];

  fs.createReadStream(csvFilePath)
    .pipe(csv({ separator: ";" }))
    .on("data", (row) => {
      // Assume que a primeira coluna do CSV contém os IDs
      ids.push(new ObjectId(row[Object.keys(row)[0]]));
    })
    .on("end", () => {
      // Agora temos a lista de IDs, podemos atualizar os status no MongoDB
      updateStatus(ids);
    });
}

// Iniciar o processo
main();
