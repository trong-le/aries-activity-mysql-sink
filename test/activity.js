import test from 'blue-tape';
import MySQLSource from '..';
import config from './test.config';
import fs from 'fs';
import knex from 'knex';

test('proper configuration', t => {
    t.equal(MySQLSource.props.name, require('../package.json').name);
    t.equal(MySQLSource.props.version, require('../package.json').version);
    t.end();
});

test('test incoming schema csv', async (t) => {
    const source = new MySQLSource();
	const fileStream = fs.createReadStream('lib/testFile');
	await source.onTaskCopy(config);
});

// test('test insert postgres', async (t) => {
//     const source = new MySQLSource();
// 	const fileStream = fs.createReadStream('lib/testFile');
// 	const connection = Object.assign({ ssl: false }, config.connection);
// 	// Get a knex database connection for setting up schema/table.
// 	const knexdb = knex({ client: 'mysql', connection });
// 	await source.insertMysql(fileStream, config, knexdb);
// 	await knexdb.destroy();
// });
