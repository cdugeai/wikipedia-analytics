# Polars implementation

## Usage

Run `make run` to execute the script.

## Results

Count infoboxes:

- Single file (1.8GB):  4.6s
- All files (32GB): 60s


Get cities:

- Single file (1.8GB):  4.7s
- All files (32GB): 62s


Get cities stats parsing columns:

- Single file (1.8GB):  6.2s
- All files (32GB): 88s

*Note:* The parsing of the columns is not optimal. Has been done with UDF and [map_elements](https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.map_elements.html) to parse the strings. 
Could be improved by using [polars.Expr.str.extract](https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.str.extract.html).

