"""The project model, the DSN helpers and instance/project settings resolution.

Everything here is pure filesystem and string handling - no database server is
needed, so this module runs anywhere the repository does.

Runnable with either ``pytest`` or ``python -m unittest``.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from graetl.config import NoProjectOpen, Settings, load_settings  # noqa: E402
from graetl.project import (  # noqa: E402
    PROJECT_FILENAME,
    Project,
    ProjectError,
    find_project,
    forget_project,
    recent_projects,
    remember_project,
    resolve_file,
)
from graetl.sdk.context import Context  # noqa: E402
from graetl.store.db import redact_dsn, rewrite_params  # noqa: E402


@contextmanager
def env(**values: str | None) -> Iterator[None]:
    """Set (``"x"``) or unset (``None``) environment variables for one test.

    ``patch.dict`` snapshots the whole mapping, so popping a key inside the
    block is restored on exit too - no test can leak a GRAETL_* variable into
    the next one, which would silently change project resolution.
    """
    with mock.patch.dict(os.environ, {}):
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield


def write_project_toml(root: Path, body: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / PROJECT_FILENAME).write_text(body, encoding="utf-8")
    return root


class TempDirTestCase(unittest.TestCase):
    """A throwaway folder per test, like the integration suite's fixture."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name).resolve()

    def tearDown(self) -> None:
        self._tmp.cleanup()


# --------------------------------------------------------------------- create


class TestProjectCreate(TempDirTestCase):
    def test_create_writes_the_whole_project_skeleton(self) -> None:
        project = Project.create(self.tmp / "wh", name="wh", title="Warehouse")

        self.assertTrue((project.root / PROJECT_FILENAME).is_file())
        self.assertTrue((project.root / ".gitignore").is_file())
        self.assertTrue(project.files_root.is_dir())
        self.assertTrue(project.pipelines_dir.is_dir())
        self.assertEqual(project.name, "wh")
        self.assertEqual(project.title, "Warehouse")
        self.assertEqual(project.target.system, "sqlite")
        # The gitignore must cover the runtime state the project produces,
        # because the folder is meant to be committed as-is.
        ignored = (project.root / ".gitignore").read_text(encoding="utf-8")
        self.assertIn("files/", ignored)
        self.assertIn(".env", ignored)
        self.assertIn("*.db", ignored)

    def test_create_refuses_to_overwrite_an_existing_project(self) -> None:
        Project.create(self.tmp / "wh")
        with self.assertRaises(ProjectError) as caught:
            Project.create(self.tmp / "wh")
        self.assertIn("already a GraETL project", str(caught.exception))

    def test_create_refuses_an_unsupported_target_system(self) -> None:
        with self.assertRaises(ProjectError) as caught:
            Project.create(self.tmp / "wh", system="mysql")
        self.assertIn("mysql", str(caught.exception))
        self.assertFalse((self.tmp / "wh" / PROJECT_FILENAME).exists())

    def test_create_refuses_postgres_without_a_dsn(self) -> None:
        with self.assertRaises(ProjectError) as caught:
            Project.create(self.tmp / "wh", system="postgres")
        self.assertIn("dsn", str(caught.exception))

    def test_create_with_postgres_records_the_dsn_and_schema(self) -> None:
        with env(GRAETL_TARGET_DSN=None):
            project = Project.create(
                self.tmp / "wh",
                system="postgres",
                dsn="postgresql://u:p@db.internal:5432/warehouse",
                schema="bookkeeping",
            )
        self.assertTrue(project.target.is_postgres)
        self.assertEqual(project.target.schema, "bookkeeping")
        self.assertEqual(project.target.dsn, "postgresql://u:p@db.internal:5432/warehouse")
        # describe() is what the UI shows, so it must never carry the password.
        self.assertNotIn("p@", project.target.describe())

    def test_a_quoted_title_survives_the_round_trip_through_toml(self) -> None:
        # The template interpolates the title into a double-quoted TOML string;
        # an unescaped quote there would produce a file that no longer parses.
        project = Project.create(self.tmp / "wh", title='The "Big" Warehouse')
        self.assertEqual(Project.load(project.root).title, 'The "Big" Warehouse')


# ----------------------------------------------------------------------- load


class TestProjectLoad(TempDirTestCase):
    def test_load_round_trips_identity_and_paths(self) -> None:
        root = write_project_toml(
            self.tmp / "wh",
            'schema = 1\n'
            '[project]\nname = "sicdb"\ntitle = "SICdb Warehouse"\n'
            'description = "beds"\nlogo = "brand.svg"\n'
            '[target]\nsystem = "sqlite"\npath = "data/warehouse.db"\n'
            '[files]\nroot = "blobs"\n'
            '[paths]\npipelines = "flows"\n',
        )
        (root / "brand.svg").write_text("<svg/>", encoding="utf-8")

        project = Project.load(root)
        self.assertEqual(project.name, "sicdb")
        self.assertEqual(project.title, "SICdb Warehouse")
        self.assertEqual(project.description, "beds")
        self.assertEqual(project.logo, "brand.svg")
        self.assertEqual(project.logo_path, root / "brand.svg")
        self.assertEqual(project.files_root, root / "blobs")
        self.assertEqual(project.pipelines_dir, root / "flows")
        self.assertEqual(project.pipeline_dir("a"), root / "flows" / "a")
        self.assertEqual(project.target.path, root / "data" / "warehouse.db")
        self.assertTrue(project.to_dict()["logo"])

    def test_load_falls_back_to_the_folder_name_and_default_paths(self) -> None:
        root = write_project_toml(self.tmp / "fallback", "schema = 1\n")
        project = Project.load(root)
        self.assertEqual(project.name, "fallback")
        self.assertEqual(project.title, "fallback")
        self.assertEqual(project.files_root, root / "files")
        self.assertEqual(project.pipelines_dir, root / "pipelines")

    def test_a_logo_beside_project_toml_is_picked_up_without_configuration(self) -> None:
        root = write_project_toml(self.tmp / "wh", 'schema = 1\n[project]\nname = "wh"\n')
        (root / "logo.png").write_bytes(b"\x89PNG")
        project = Project.load(root)
        self.assertEqual(project.logo, "logo.png")
        self.assertEqual(project.logo_path, root / "logo.png")

    def test_a_configured_logo_that_is_absent_reports_no_logo_path(self) -> None:
        root = write_project_toml(
            self.tmp / "wh", 'schema = 1\n[project]\nlogo = "missing.png"\n'
        )
        project = Project.load(root)
        self.assertEqual(project.logo, "missing.png")
        self.assertIsNone(project.logo_path)
        self.assertFalse(project.to_dict()["logo"])

    def test_load_rejects_a_project_written_by_a_newer_graetl(self) -> None:
        root = write_project_toml(self.tmp / "wh", "schema = 99\n")
        with self.assertRaises(ProjectError) as caught:
            Project.load(root)
        message = str(caught.exception)
        self.assertIn("99", message)
        self.assertIn("newer GraETL", message)

    def test_load_says_clearly_when_project_toml_is_missing(self) -> None:
        (self.tmp / "empty").mkdir()
        with self.assertRaises(ProjectError) as caught:
            Project.load(self.tmp / "empty")
        self.assertIn(PROJECT_FILENAME, str(caught.exception))

    def test_load_rejects_an_unsupported_target_system(self) -> None:
        root = write_project_toml(
            self.tmp / "wh", 'schema = 1\n[target]\nsystem = "oracle"\n'
        )
        with self.assertRaises(ProjectError) as caught:
            Project.load(root)
        self.assertIn("oracle", str(caught.exception))

    def test_relative_and_absolute_paths_are_both_honoured(self) -> None:
        elsewhere = self.tmp / "outside"
        elsewhere.mkdir()
        root = write_project_toml(
            self.tmp / "wh",
            "schema = 1\n"
            f'[files]\nroot = "{elsewhere.as_posix()}/blobs"\n'
            f'[paths]\npipelines = "{elsewhere.as_posix()}/flows"\n',
        )
        project = Project.load(root)
        self.assertEqual(project.files_root, elsewhere / "blobs")
        self.assertEqual(project.pipelines_dir, elsewhere / "flows")

        relative = write_project_toml(
            self.tmp / "rel",
            'schema = 1\n[files]\nroot = "sub/blobs"\n[paths]\npipelines = "sub/flows"\n',
        )
        project = Project.load(relative)
        self.assertEqual(project.files_root, relative / "sub" / "blobs")
        self.assertEqual(project.pipelines_dir, relative / "sub" / "flows")
        project.ensure_dirs()
        self.assertTrue((relative / "sub" / "blobs").is_dir())
        self.assertTrue((relative / "sub" / "flows").is_dir())

    def test_find_project_walks_upwards_like_git(self) -> None:
        root = write_project_toml(self.tmp / "wh", "schema = 1\n")
        deep = root / "pipelines" / "a" / "modules"
        deep.mkdir(parents=True)
        self.assertEqual(find_project(deep), root)
        self.assertIsNone(find_project(self.tmp / "outside"))


# ------------------------------------------------------------ DSN expansion


class TestDsnExpansion(TempDirTestCase):
    def project_with_dsn(self, dsn: str, *, env_file: str | None = None) -> Path:
        root = write_project_toml(
            self.tmp / "wh",
            f'schema = 1\n[target]\nsystem = "postgres"\ndsn = "{dsn}"\n',
        )
        if env_file is not None:
            (root / ".env").write_text(env_file, encoding="utf-8")
        return root

    def test_a_variable_is_expanded_from_the_environment(self) -> None:
        root = self.project_with_dsn("postgresql://u:${PGPASSWORD}@h/db")
        with env(PGPASSWORD="s3cret", GRAETL_TARGET_DSN=None):
            self.assertEqual(Project.load(root).target.dsn, "postgresql://u:s3cret@h/db")

    def test_a_variable_falls_back_to_the_dot_env_file(self) -> None:
        root = self.project_with_dsn(
            "postgresql://u:${PGPASSWORD}@h/db",
            env_file="# secrets\nPGPASSWORD = from-dotenv\n",
        )
        with env(PGPASSWORD=None, GRAETL_TARGET_DSN=None):
            self.assertEqual(Project.load(root).target.dsn, "postgresql://u:from-dotenv@h/db")

    def test_a_quoted_dot_env_value_loses_its_quotes(self) -> None:
        root = self.project_with_dsn(
            "postgresql://u:${PGPASSWORD}@h/db", env_file='PGPASSWORD="qu oted"\n'
        )
        with env(PGPASSWORD=None, GRAETL_TARGET_DSN=None):
            self.assertEqual(Project.load(root).target.dsn, "postgresql://u:qu oted@h/db")

    def test_the_environment_wins_over_the_dot_env_file(self) -> None:
        root = self.project_with_dsn(
            "postgresql://u:${PGPASSWORD}@h/db", env_file="PGPASSWORD=from-dotenv\n"
        )
        with env(PGPASSWORD="from-environment", GRAETL_TARGET_DSN=None):
            self.assertEqual(
                Project.load(root).target.dsn, "postgresql://u:from-environment@h/db"
            )

    def test_an_unset_variable_is_left_intact_so_the_error_names_it(self) -> None:
        root = self.project_with_dsn("postgresql://u:${NOT_SET_ANYWHERE}@h/db")
        with env(NOT_SET_ANYWHERE=None, GRAETL_TARGET_DSN=None):
            dsn = Project.load(root).target.dsn
        self.assertIn("${NOT_SET_ANYWHERE}", dsn)

    def test_graetl_target_dsn_overrides_project_toml_entirely(self) -> None:
        root = self.project_with_dsn("postgresql://configured@h/db")
        with env(GRAETL_TARGET_DSN="postgresql://override:${PW}@other/db2", PW="p"):
            self.assertEqual(Project.load(root).target.dsn, "postgresql://override:p@other/db2")

    def test_a_postgres_target_without_any_dsn_is_refused(self) -> None:
        root = write_project_toml(
            self.tmp / "wh", 'schema = 1\n[target]\nsystem = "postgres"\n'
        )
        with env(GRAETL_TARGET_DSN=None):
            with self.assertRaises(ProjectError) as caught:
                Project.load(root)
        self.assertIn("dsn", str(caught.exception))


# --------------------------------------------------------------- file access


class TestFileResolution(TempDirTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.project = Project.create(self.tmp / "wh")

    def test_a_normal_nested_path_resolves_under_the_file_root(self) -> None:
        path = resolve_file(self.project, "dicom", "study-1", "series.dcm")
        self.assertEqual(path, self.project.files_root / "dicom" / "study-1" / "series.dcm")

    def test_dot_dot_segments_are_refused(self) -> None:
        with self.assertRaises(ProjectError):
            resolve_file(self.project, "..", "escape.txt")
        with self.assertRaises(ProjectError):
            # A traversal hidden in the middle of an otherwise innocent name.
            resolve_file(self.project, "dicom", "..", "..", "escape.txt")

    def test_an_absolute_path_is_refused(self) -> None:
        with self.assertRaises(ProjectError):
            resolve_file(self.project, "/etc/passwd")

    def test_the_file_root_itself_is_allowed(self) -> None:
        self.assertEqual(resolve_file(self.project), self.project.files_root)

    def context(self, files_root: Path | None) -> Context:
        return Context(
            pipeline_id="p",
            pipeline_title="P",
            run_id=None,
            mode="incremental",
            dir=self.tmp,
            config={},
            files_root=files_root,
        )

    def test_ctx_file_creates_the_parent_and_refuses_traversal(self) -> None:
        ctx = self.context(self.project.files_root)
        path = ctx.file("exports", "2026", "rows.csv")
        self.assertEqual(path, self.project.files_root / "exports" / "2026" / "rows.csv")
        self.assertTrue(path.parent.is_dir())

        with self.assertRaises(ValueError):
            ctx.file("..", "escape.txt")
        with self.assertRaises(ValueError):
            ctx.file("/etc/passwd")

    def test_ctx_file_explains_itself_when_there_is_no_file_root(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            self.context(None).file("x.txt")
        self.assertIn("file root", str(caught.exception))


# ----------------------------------------------------------- recent projects


class TestRecentProjects(TempDirTestCase):
    """The recent list is per user, so it is redirected at a temp folder here.

    Without the patch these tests would write into the real user profile.
    """

    def setUp(self) -> None:
        super().setUp()
        self.state_dir = self.tmp / "state"
        patcher = mock.patch("graetl.project.user_state_dir", return_value=self.state_dir)
        patcher.start()
        self.addCleanup(patcher.stop)

    def make(self, name: str) -> Project:
        return Project.create(self.tmp / name, name=name)

    def test_an_empty_or_missing_list_reads_as_empty(self) -> None:
        self.assertEqual(recent_projects(), [])
        self.state_dir.mkdir(parents=True)
        (self.state_dir / "recent.json").write_text("not json at all", encoding="utf-8")
        self.assertEqual(recent_projects(), [])
        (self.state_dir / "recent.json").write_text('{"not": "a list"}', encoding="utf-8")
        self.assertEqual(recent_projects(), [])

    def test_projects_come_back_most_recently_remembered_first(self) -> None:
        a, b = self.make("alpha"), self.make("beta")
        remember_project(a)
        remember_project(b)
        self.assertEqual([e["name"] for e in recent_projects()], ["beta", "alpha"])
        self.assertEqual(recent_projects()[0]["root"], str(b.root))
        self.assertEqual(recent_projects()[0]["target"], "sqlite")

    def test_remembering_the_same_project_twice_moves_it_without_duplicating(self) -> None:
        a, b = self.make("alpha"), self.make("beta")
        remember_project(a)
        remember_project(b)
        remember_project(a)
        self.assertEqual([e["name"] for e in recent_projects()], ["alpha", "beta"])

    def test_a_project_whose_folder_is_gone_is_skipped(self) -> None:
        a, b = self.make("alpha"), self.make("beta")
        remember_project(a)
        remember_project(b)
        (b.root / PROJECT_FILENAME).unlink()
        self.assertEqual([e["name"] for e in recent_projects()], ["alpha"])

    def test_the_list_is_capped(self) -> None:
        for index in range(5):
            remember_project(self.make(f"p{index}"), limit=3)
        names = [e["name"] for e in recent_projects()]
        self.assertEqual(names, ["p4", "p3", "p2"])

    def test_forgetting_a_project_removes_only_that_entry(self) -> None:
        a, b = self.make("alpha"), self.make("beta")
        remember_project(a)
        remember_project(b)
        forget_project(b.root)
        self.assertEqual([e["name"] for e in recent_projects()], ["alpha"])
        # Forgetting something that was never remembered is a no-op.
        forget_project(self.tmp / "never")
        self.assertEqual([e["name"] for e in recent_projects()], ["alpha"])


# ------------------------------------------------------------- DSN redaction


class TestRedactDsn(unittest.TestCase):
    def test_a_url_dsn_loses_its_password(self) -> None:
        self.assertEqual(
            redact_dsn("postgresql://etl:s3cret@db.internal:5432/warehouse"),
            "postgresql://etl@db.internal:5432/warehouse",
        )

    def test_a_key_value_dsn_loses_its_password(self) -> None:
        redacted = redact_dsn("host=db user=etl password=s3cret dbname=warehouse")
        self.assertNotIn("s3cret", redacted)
        self.assertEqual(redacted, "host=db user=etl dbname=warehouse")
        # Case in the key must not let a password through.
        self.assertNotIn("s3cret", redact_dsn("host=db PASSWORD=s3cret"))

    def test_a_dsn_with_no_password_is_left_alone(self) -> None:
        self.assertEqual(
            redact_dsn("postgresql://etl@db.internal/warehouse"),
            "postgresql://etl@db.internal/warehouse",
        )
        self.assertEqual(
            redact_dsn("postgresql://db.internal/warehouse"),
            "postgresql://db.internal/warehouse",
        )
        self.assertEqual(redact_dsn("host=db dbname=warehouse"), "host=db dbname=warehouse")
        self.assertEqual(redact_dsn(""), "")


# ----------------------------------------------------------- param rewriting


class TestRewriteParams(unittest.TestCase):
    def test_placeholders_become_format_style(self) -> None:
        self.assertEqual(
            rewrite_params("SELECT * FROM t WHERE a = ? AND b = ?"),
            "SELECT * FROM t WHERE a = %s AND b = %s",
        )

    def test_a_question_mark_inside_a_literal_is_left_alone(self) -> None:
        self.assertEqual(
            rewrite_params("SELECT '?' AS q, ? AS p"),
            "SELECT '?' AS q, %s AS p",
        )

    def test_a_question_mark_inside_a_quoted_identifier_is_left_alone(self) -> None:
        self.assertEqual(
            rewrite_params('SELECT "weird?column" FROM t WHERE a = ?'),
            'SELECT "weird?column" FROM t WHERE a = %s',
        )

    def test_a_question_mark_inside_comments_is_left_alone(self) -> None:
        self.assertEqual(
            rewrite_params("-- is this ? a parameter\nSELECT ?"),
            "-- is this ? a parameter\nSELECT %s",
        )
        self.assertEqual(
            rewrite_params("/* not ? here */ SELECT ? /* nor ? here */"),
            "/* not ? here */ SELECT %s /* nor ? here */",
        )
        # An unterminated comment must not swallow the rest as a parameter.
        self.assertEqual(rewrite_params("SELECT ? -- trailing ?"), "SELECT %s -- trailing ?")

    def test_a_doubled_quote_escape_does_not_end_the_literal(self) -> None:
        self.assertEqual(
            rewrite_params("SELECT 'it''s ? fine' AS q, ? AS p"),
            "SELECT 'it''s ? fine' AS q, %s AS p",
        )

    def test_percent_signs_are_doubled_so_the_result_is_valid_format_sql(self) -> None:
        rewritten = rewrite_params("SELECT a % b FROM t WHERE c = ?")
        self.assertEqual(rewritten, "SELECT a %% b FROM t WHERE c = %s")
        # The driver interpolates with %; the doubling must survive that.
        self.assertEqual(rewritten % ("'x'",), "SELECT a % b FROM t WHERE c = 'x'")

    def test_a_like_pattern_survives_interpolation(self) -> None:
        rewritten = rewrite_params("SELECT * FROM t WHERE note LIKE '%x%' AND id = ?")
        self.assertEqual(
            rewritten % ("7",), "SELECT * FROM t WHERE note LIKE '%x%' AND id = 7"
        )
        # ... including the pathological literal a regex would corrupt.
        self.assertEqual(
            rewrite_params("WHERE note LIKE '%?%' AND id = ?") % ("7",),
            "WHERE note LIKE '%?%' AND id = 7",
        )


# ----------------------------------------------------------------- settings


class TestSettingsResolution(TempDirTestCase):
    def setUp(self) -> None:
        super().setUp()
        # An instance root that is not itself a project, and an empty folder for
        # GRAETL_ROOT, so nothing resolves by accident from the real repository.
        self.instance = self.tmp / "instance"
        self.instance.mkdir()
        self.nowhere = self.tmp / "nowhere"
        self.nowhere.mkdir()

    def make_project(self, name: str) -> Project:
        return Project.create(self.tmp / name, name=name)

    def write_instance_config(self, body: str) -> None:
        (self.instance / "graetl.toml").write_text(body, encoding="utf-8")

    def test_an_explicit_path_wins_over_everything_else(self) -> None:
        explicit = self.make_project("explicit")
        from_env = self.make_project("from_env")
        configured = self.make_project("configured")
        self.write_instance_config(f'[project]\npath = "{configured.root.as_posix()}"\n')
        with env(GRAETL_PROJECT=str(from_env.root), GRAETL_ROOT=str(self.nowhere)):
            settings = load_settings(self.instance, project=explicit.root)
        self.assertEqual(settings.require_project().name, "explicit")

    def test_graetl_project_wins_over_the_instance_config(self) -> None:
        from_env = self.make_project("from_env")
        configured = self.make_project("configured")
        self.write_instance_config(f'[project]\npath = "{configured.root.as_posix()}"\n')
        with env(GRAETL_PROJECT=str(from_env.root), GRAETL_ROOT=str(self.nowhere)):
            settings = load_settings(self.instance)
        self.assertEqual(settings.require_project().name, "from_env")

    def test_the_instance_config_wins_over_walking_up(self) -> None:
        configured = self.make_project("configured")
        # A project sitting above the instance root would otherwise be found by
        # the upward walk; an explicit [project] path must take precedence.
        write_project_toml(self.instance, 'schema = 1\n[project]\nname = "inline"\n')
        self.write_instance_config(f'[project]\npath = "{configured.root.as_posix()}"\n')
        with env(GRAETL_PROJECT=None, GRAETL_ROOT=str(self.nowhere)):
            settings = load_settings(self.instance)
        self.assertEqual(settings.require_project().name, "configured")

    def test_a_relative_configured_path_is_taken_from_the_instance_root(self) -> None:
        self.make_project("sibling")
        self.write_instance_config('[project]\npath = "../sibling"\n')
        with env(GRAETL_PROJECT=None, GRAETL_ROOT=str(self.nowhere)):
            settings = load_settings(self.instance)
        self.assertEqual(settings.require_project().name, "sibling")

    def test_walking_up_from_the_instance_root_finds_the_project(self) -> None:
        above = write_project_toml(self.tmp / "above", 'schema = 1\n[project]\nname = "above"\n')
        nested = above / "deep" / "instance"
        nested.mkdir(parents=True)
        (nested / "graetl.toml").write_text("[server]\nport = 8999\n", encoding="utf-8")
        with env(GRAETL_PROJECT=None, GRAETL_ROOT=str(self.nowhere)):
            settings = load_settings(nested)
        self.assertEqual(settings.require_project().name, "above")

    def test_no_project_anywhere_leaves_settings_without_one(self) -> None:
        with env(GRAETL_PROJECT=None, GRAETL_ROOT=str(self.nowhere)):
            settings = load_settings(self.instance)
        self.assertFalse(settings.has_project)
        self.assertIsNone(settings.project)

    def test_require_project_raises_when_no_project_is_open(self) -> None:
        with env(GRAETL_PROJECT=None, GRAETL_ROOT=str(self.nowhere)):
            with self.assertRaises(NoProjectOpen):
                load_settings(self.instance, require_project=True)

    def test_settings_require_project_raises_when_absent(self) -> None:
        settings = Settings(root=self.tmp)
        with self.assertRaises(NoProjectOpen) as caught:
            settings.require_project()
        self.assertIn("GRAETL_PROJECT", str(caught.exception))
        # Every project-derived accessor goes through the same gate.
        for accessor in ("target", "pipelines_dir", "files_root"):
            with self.assertRaises(NoProjectOpen):
                getattr(settings, accessor)

    def test_a_project_runtime_block_overrides_the_instance_one(self) -> None:
        project = self.make_project("tuned")
        (project.root / PROJECT_FILENAME).write_text(
            'schema = 1\n[project]\nname = "tuned"\n'
            '[target]\nsystem = "sqlite"\npath = "warehouse.db"\n'
            "[runtime]\nparallel_modules = 4\n",
            encoding="utf-8",
        )
        self.write_instance_config("[runtime]\nparallel_modules = 1\nentity_batch_size = 25\n")
        with env(
            GRAETL_PROJECT=str(project.root),
            GRAETL_ROOT=str(self.nowhere),
            GRAETL_PARALLEL_MODULES=None,
        ):
            settings = load_settings(self.instance)
        self.assertEqual(settings.parallel_modules, 4)
        # ... and leaves the instance's own value alone where it says nothing.
        self.assertEqual(settings.entity_batch_size, 25)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
