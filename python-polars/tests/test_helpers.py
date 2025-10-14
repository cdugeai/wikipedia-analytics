import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers import (
    parse_postal_code,
    parse_population_and_year,
    parse_density,
    parse_superficy,
    parse_maire,
    parse_coords,
)


def test_parse_postal_code():
    assert parse_postal_code("44") == 44
    assert parse_postal_code("2222") == 2222
    # Select first matching
    assert parse_postal_code("3333 4444") == 3333
    # If no match, return None
    assert parse_postal_code("abc") is None
    assert parse_postal_code("abc 45") == 45
    assert parse_postal_code("abc_55") == 55
    assert parse_postal_code("abc_5_5") == 5


def test_parse_population_and_year():
    assert parse_population_and_year("1 637 hab. (2017)") == (1637, 2017)
    assert parse_population_and_year(
        "3 752 hab. (2021 en augmentation de 0,4 % par rapport à 2015)"
    ) == (3752, 2021)
    assert parse_population_and_year(
        "3 752 hab. ( 2021 en augmentation de 0,4 % par rapport à 2015)"
    ) == (3752, 2021)
    assert parse_population_and_year("3 752 hab. ()") == (3752, None)
    assert parse_population_and_year("05 hab. (2020)") == (5, 2020)
    assert parse_population_and_year("05 hab. (2020 2024)") == (5, 2020)
    assert parse_population_and_year("some (text)") == (None, None)


def test_parse_density():
    assert parse_density("2,1 hab./km 2") == 2.1
    assert parse_density(" hab./km 2") is None
    assert parse_density("34 hab./km 2") == 34
    assert parse_density("220 hab./km 2") == 220
    assert parse_density("22 046 hab./km 2") == 22046
    assert parse_density("22 046 777 hab./km 2") == 22046777
    assert parse_density("34 hab") is None


def test_parse_superficy():
    assert parse_superficy("12,30 km 2") == 12.3
    assert parse_superficy("100,5 km 2") == 100.50
    assert parse_superficy("76 km 2") == 76
    assert parse_superficy("6 332,6 km 2") == 6332.6
    assert parse_superficy("10 030 km 2") == 10030


def test_parse_maire():
    assert parse_maire("Abdou Rachadi 2020 -2026") == ("Abdou Rachadi", 2020, 2026)
    assert parse_maire("Albert Bob 2020 -") == ("Albert Bob", 2020, None)
    assert parse_maire("Albert Bob 2021 -2025") == ("Albert Bob", 2021, 2025)
    assert parse_maire("Tuhoe Tekurio 2014 -2020") == ("Tuhoe Tekurio", 2014, 2020)
    assert parse_maire("Gaston Tong Sang (Tapura huiraatira) 2020 -2026") == (
        "Gaston Tong Sang (Tapura huiraatira)",
        2020,
        2026,
    )


def test_parse_coords():
    assert parse_coords("16° 03′ 05″ sud, 145° 39′ 30″ ouest") == (-16.051, -145.658)
    assert parse_coords("48° 44′ 37″ nord, 2° 48′ 49″ est") == (48.744, 2.814)
    assert parse_coords("48° 44′ 37″ sud, 2° 48′ 49″ est") == (-48.744, 2.814)
    assert parse_coords("48° 44′ 37″ nord, 2° 48′ 49″ ouest") == (48.744, -2.814)
