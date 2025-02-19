import type { MigrationConfig} from '~/migrator';
import { readMigrationFiles } from '~/migrator';
import type { NeonDatabase } from './driver';
import { NeonQueryResultHKT } from './session';

export async function migrate(db: NeonDatabase, config: string | MigrationConfig) {
	const migrations = readMigrationFiles(config);
	await db.dialect.migrate(migrations, db.session);
}
