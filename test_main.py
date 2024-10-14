import subprocess


def test_extract():
    """Test extract()"""
    result = subprocess.run(
        ["python", "main.py", "extract"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Extracting data..." in result.stdout


def test_load():
    """Test transform_load()"""
    result = subprocess.run(
        ["python", "main.py", "load"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Transforming data..." in result.stdout


def test_query():
    """tests general_query"""
    result = subprocess.run(
        [
            "python",
            "main.py",
            "query",
            """
            select aa.state, sum(aa.total_population) state_population, sum(bb.violend) violence_arrest
 from `ids706_data_engineering`.`default`.`ar805_population_db` aa
left join ids706_data_engineering.default.ar805_arrest_db bb 
on aa.country = bb.country
group by aa.state
having violence_arrest is not null 
order by violence_arrest desc;
            """,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


if __name__ == "__main__":
    test_extract()
    test_load()
    test_query()
