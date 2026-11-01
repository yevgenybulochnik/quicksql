from quicksql.scaffold import TEMPLATES, template_content, template_names, user_templates


def test_no_user_templates_dir_means_builtin_only() -> None:
    assert user_templates() == {}
    assert template_names() == ["base"]


def test_user_templates_discovered_from_the_quicksql_home(qsql_home) -> None:
    tdir = qsql_home / "templates"
    tdir.mkdir(parents=True)
    (tdir / "sales.qsql").write_text("-- @cell s\nSELECT 1;\n")
    (tdir / "legacy.qsql.sql").write_text("-- @cell l\nSELECT 2;\n")
    (tdir / "schema_dump.sql").write_text("CREATE TABLE noise (id INT);\n")
    (tdir / "readme.txt").write_text("not a template\n")

    assert set(user_templates()) == {"sales", "legacy"}  # plain .sql/.txt ignored
    assert template_names() == ["base", "legacy", "sales"]
    assert template_content("sales") == "-- @cell s\nSELECT 1;\n"
    assert template_content("base") == TEMPLATES["base"]
