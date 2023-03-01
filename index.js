const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require("./config");
const massive = require("massive");
const monitor = require("pg-monitor");
const axios = require("axios");
const url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";

// Call start
(async () => {
  console.log("main.js: before start");

  const db = await massive(
    {
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    },
    {
      // Massive Configuration
      scripts: process.cwd() + "/migration",
      allowedSchemas: [DATABASE_SCHEMA],
      whitelist: [`${DATABASE_SCHEMA}.%`],
      excludeFunctions: true,
    },
    {
      // Driver Configuration
      noWarnings: true,
      error: function (err, client) {
        console.log(err);
        process.emit("uncaughtException", err);
        throw err;
      },
    }
  );

  if (!monitor.isAttached() && SHOW_PG_MONITOR === "true") {
    monitor.attach(db.driverConfig);
  }

  const execFileSql = async (schema, type) => {
    return new Promise(async (resolve) => {
      const objects = db["user"][type];

      if (objects) {
        for (const [key, func] of Object.entries(objects)) {
          console.log(`executing ${schema} ${type} ${key}...`);
          await func({
            schema: DATABASE_SCHEMA,
          });
        }
      }

      resolve();
    });
  };

  //public
  const migrationUp = async () => {
    return new Promise(async (resolve) => {
      await execFileSql(DATABASE_SCHEMA, "schema");

      //cria as estruturas necessarias no db (schema)
      await execFileSql(DATABASE_SCHEMA, "table");
      await execFileSql(DATABASE_SCHEMA, "view");

      console.log(`reload schemas ...`);
      await db.reload();

      resolve();
    });
  };

  const getData = async () => {
    try {
      const api_data = await axios.get(url);
      return api_data.data.data; // Yes, data is inside data
    } catch (err) {
      console.log(err);
    }
  };

  try {
    await migrationUp();

    const data = await getData();

    data.forEach(async (item) => {
      await db.query(
        "INSERT INTO otaviocantareli.api_data (doc_record) VALUES ($1)",
        [item]
      );
    });

    // Results using Node function
    const resultNode = await db.query(
      "select DISTINCT(doc_record) from otaviocantareli.api_data"
    );
    let populationNode = 0;
    resultNode.forEach((item) => {
      if (
        item.doc_record["Year"] == 2020 ||
        item.doc_record["Year"] == 2019 ||
        item.doc_record["Year"] == 2018
      ) {
        populationNode += item.doc_record["Population"];
      }
    });
    console.log(`Result using Node function: ${populationNode}`);

    // Results using PostgreSQL
    const resultPg = await db.query(
      `select SUM(CAST(doc_record->>'Population' as INTEGER)) from (
				select DISTINCT(doc_record) from otaviocantareli.api_data where doc_record->>'Year' in ('2018', '2019', '2020')
			) as "result"`
    );
    console.log(`Results using PostgreSQL ${Object.values(resultPg[0])}`);
  } catch (e) {
    console.log(e.message);
  } finally {
    console.log("finally");
  }
  console.log("main.js: after start");
})();
