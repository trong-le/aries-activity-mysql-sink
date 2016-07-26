import { Activity, singleS3StreamInput } from 'aries-data';
import DropSchemaStrategy from './DropSchemaStrategy';
import AlterSchemaStrategy from './AlterSchemaStrategy';
import inferSchema from './util/inferSchema';
// import jsonSchemaGenerator from 'json-schema-generator';
import fs from 'fs';
import knex from 'knex';
import _ from 'lodash';
import _deep from "lodash-deep";
_.mixin(_deep);
import highland from 'highland';
import mysql from 'mysql';
// import file from './testFile'
// Use _.range to create batch sizes?

export default class MySQLSource extends Activity {
    static props = {
        name: require('../package.json').name,
        version: require('../package.json').version,
    };

    @singleS3StreamInput()
    async onTask(activityTask, config) {
        // Infer the incoming schema.
        const incomingSchema = await inferSchema(activityTask.input.file, config.schemaHint, config.json);

        // Create redshift connection object from configuration.
        const connection = Object.assign({ ssl: true }, config.connection);

        // Get a database connection.
        const knexdb = knex({ client: 'mysql', connection });

        await knexdb.schema.raw(`CREATE SCHEMA IF NOT EXISTS ${config.schema};`);

        // Produce a schema preparer.
        const schemaPreparer = config.drop
        ? new DropSchemaStrategy(knexdb)
        : new AlterSchemaStrategy(knexdb);

        // Prepare the schema for the COPY.
        await schemaPreparer.prepare(config, incomingSchema);

        const mySqlConnection = mysql.createConnection(config.connection);
        mySqlConnection.connect();

        // Create the COPY command.
        const filename = activityTask.input.key;
        await this.insertMysql(incomingSchema, filename, config, knexdb);

        // Tear down connection pool.
        await mySqlConnection.end();
        await knexdb.destroy();
        this.log.info('Sucessfully destroyed connection pool.');
    }

    // async onTaskCopy(config) {
    //     // Infer the incoming schema.
    //     const incomingSchema = await inferSchema(fileStream, config.schemaHint, config.json);

    //     // Create redshift connection object from configuration.
    //     const connection = Object.assign({ ssl: false }, config.connection);

    //     // Get a knex database connection for setting up schema/table.
    //     const knexdb = knex({ client: 'mysql', connection });

    //     await knexdb.schema.raw(`CREATE SCHEMA IF NOT EXISTS ${config.schema};`);

    //     // Produce a schema preparer.
    //     const schemaPreparer = config.drop
    //     ? new DropSchemaStrategy(knexdb)
    //     : new AlterSchemaStrategy(knexdb);

    //     // Prepare the schema for the INSERT.
    //     await schemaPreparer.prepare(config, incomingSchema);

    //     const mySqlConnection = mysql.createConnection(config.connection);
    //     mySqlConnection.connect();

    //     // Create the INSERT command.
    //     // const filename = activityTask.input.key;

    //     // Get column names and parse JSON/CSV data
    //     // Wanna make col/value pair for each db.task command

    //     await this.insertMysql(incomingSchema, fileStream, config, knexdb);

    //     // Execute the command.
    //     this.log.info('Sucessfully ran INSERT to MySQL database.');

    //     // connection.destroy();
    //     await mySqlConnection.end();
    //     await knexdb.destroy();
    // }

    async insertMysql(incomingSchema, filename, config, knexdb) {
        const fileStream = fs.createReadStream(filename);
        // Create batch of 1000, for each batch check schema
        const batchStream = highland(fileStream).batch(1000);

        // batchStream.consume((err, batch, push, next) => {
        // inferSchema is working but nothing inside of batchSchema

        const columnNames = incomingSchema.map(field => field.name);
        // this.log.info('columnNames: ' + columnNames);
        let sql = 'INSERT INTO (??) VALUES (?)';

        // Generate array of objects representing all rows.
        batchStream.on('data', (batch) => {
            this.log.info('inside data');
            // let valueSet = batch.getRows().map((row) => {
            //     let colValPair = fields.map((f, i) => {
            //         let val = _.deepGet(row, f);
            //         // If val is undefined, switch it to null for redshift.
            //         if (_.isUndefined(val)) val = null;
            //         // ex: { context_page_title: 'Astronomer' }
            //         return { [columnNames[i]]: val };
            //     });
            //     // Merge all hash pairs to form one object for one row.
            //     return _.merge.apply(null, colValPair);
            // })
            // const insertValues = [columnNames, batch];
            // sql = mysql.format(sql, insertValues);
            // this.log.info(sql);
        });

        //     next();

        // }).done(connection.end());
    }
};
