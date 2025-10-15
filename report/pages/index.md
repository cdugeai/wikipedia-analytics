---
title: Some Wikipedia Stats
---

## Movies in Wikipedia ?


```sql countries
  select
      country
  from files.films
  group by country
```

```sql genres
  select
      genre
  from files.films
  group by genre
```

<Dropdown data={countries} name=country value=country >
    <DropdownOption value="%" valueLabel="All Coutries"/>
</Dropdown>

<Dropdown data={genres} name=genre value=genre>
    <DropdownOption value="%" valueLabel="All Genres"/>
</Dropdown>


```sql all_durations
  select 
  genre, country, duration
  from files.films
  where genre LIKE '${inputs.genre.value}'
  and country LIKE '${inputs.country.value}'
```


```sql boxplot_data
  select 
    name,intervalBottom,midpoint,intervalTop
  from files.boxplot_sample
  --where genre LIKE '${inputs.genre.value}'
  --and country LIKE '${inputs.country.value}'
```


<BarChart
    data={all_durations}
    title="Average movie duration, {inputs.country.label}"
    x=genre
    y=duration
    series=country
    type=grouped
    yAxisTitle="minutes"
/>


<BoxPlot 
    data={boxplot_data}
    title="Movie duration, aggregated for {inputs.country.label} and {inputs.genre.label}"
    name=name
    intervalBottom=intervalBottom
    midpoint=midpoint
    intervalTop=intervalTop
    yFmt=usd0
/>