import unittest

import tap_snowflake.sync_strategies.common as common


class TestUnloadSql(unittest.TestCase):
    """Unit tests for the Snowflake unload SQL generation (no live connection required)."""

    SELECT_SQL = 'SELECT "C_PK" FROM "DB"."SCHEMA"."TABLE"'
    S3_FOLDER = {'bucket': 'my-bucket', 'key': 'import/123'}
    S3_CREDS = {'accessKeyID': 'AKIA', 'secretKey': 'SECRET', 'sessionToken': 'TOKEN'}

    def test_common_unload_line_includes_overwrite(self):
        """The shared unload copy options must request OVERWRITE so re-runs replace stale files."""
        self.assertIn('OVERWRITE = TRUE', common.get_common_line_for_unload())

    def test_named_stage_unload_includes_overwrite(self):
        """
        Regression for WP-31676: unloading to the internal named stage must pass OVERWRITE = TRUE
        so a re-run after an interrupted run does not fail with
        "Files already existing at the unload destination ... Use overwrite option to force unloading."
        """
        copy_sql = common.generate_copy_sql_external_unload(
            self.SELECT_SQL, self.S3_FOLDER, stage_name='import_abc_123')

        self.assertTrue(copy_sql.startswith('COPY INTO @import_abc_123 FROM'))
        self.assertIn('OVERWRITE = TRUE', copy_sql)

    def test_external_location_unload_includes_overwrite(self):
        """The direct-to-S3 unload path must also be overwrite-safe for consistency."""
        copy_sql = common.generate_copy_sql_external_unload(
            self.SELECT_SQL, self.S3_FOLDER, temp_s3_creds=self.S3_CREDS)

        self.assertTrue(copy_sql.startswith("COPY INTO 's3://my-bucket/import/123/' FROM"))
        self.assertIn('OVERWRITE = TRUE', copy_sql)


if __name__ == '__main__':
    unittest.main()
