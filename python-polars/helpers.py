import re


def parse_postal_code(str_):
    match_ = re.search(r"\d+\.?\d*", str_)
    if match_ is None:
        return None
    else:
        return int(match_.group())


def parse_population_and_year(str_):
    # Extract number (all digits and spaces before "hab.")
    number_match = re.search(r"([\d\s]+)\s*hab\.", str_)
    population = int(number_match.group(1).replace(" ", "")) if number_match else None

    # Extract year from parentheses
    year_match = re.search(r"\(([^)]*?(\d{4}))", str_)
    year = int(year_match.group(1)) if year_match else None

    return population, year


def parse_density(str_):
    number_match = re.search(r"(\d+(?:[,.]\d+)?)hab\./km2", str_.replace(" ", ""))
    density = float(number_match.group(1).replace(",", ".")) if number_match else None

    return density


def parse_superficy(str_):
    number_match = re.search(r"(\d+(?:[,.]\d+)?)km2", str_.replace(" ", ""))
    superficy = float(number_match.group(1).replace(",", ".")) if number_match else None

    return superficy


def parse_maire(str_):
    name_match = re.search(r"^(.+?)\s+\d{4}", str_)
    name = name_match.group(1) if name_match else None

    start_match = re.search(r"^.+?\s+(\d{4})\s*", str_)
    start_year = int(start_match.group(1)) if start_match else None

    end_match = re.search(r"-\s*(\d{4})$", str_)
    end_year = int(end_match.group(1)) if end_match else None
    #return {"name": name, "start_year": start_year, "end_year": end_year}
    return (name, start_year, end_year)


def parse_coords(str_):
    match = re.match(
        r"(\d+)°\s*(\d+)′\s*(\d+)″\s*(nord|sud),\s*(\d+)°\s*(\d+)′\s*(\d+)″\s*(est|ouest)",
        str_,
        re.IGNORECASE,
    )

    if match:
        lat = round(
            (
                int(match.group(1))
                + int(match.group(2)) / 60
                + int(match.group(3)) / 3600
            ),
            3,
        )
        lon = round(
            (
                int(match.group(5))
                + int(match.group(6)) / 60
                + int(match.group(7)) / 3600
            ),
            3,
        )

        if match.group(4).lower() == "sud":
            lat *= -1
        if match.group(8).lower() == "ouest":
            lon *= -1
        return (lat, lon)
    else:
        return (None, None)
