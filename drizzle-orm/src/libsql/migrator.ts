import { MigrationConfig, readMigrationFiles } from '~/migrator';
import type { DrizzleLibSQLDatabase } from './driver';

export async function migrate(db: DrizzleLibSQLDatabase, config: string | MigrationConfig) {
	const migrations = readMigrationFiles(config);
	await db.dialect.migrate(migrations, db.session);
}
