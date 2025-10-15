---
title: Some Wikipedia Stats
---

# Some stats

```sql stats
  select
      n_articles, total_char, avg_char_per_article, skyscrapers_total
  from files.general_stats
```



Here are some statistics about the content of French Wikipedia.

<BigValue 
  data={stats} 
  value=n_articles
  title="Articles"
  fmt='#,##0.000,,"M"'
/>

<BigValue 
  data={stats} 
  value=total_char
  title="Total characters"
  fmt=num2b
/>

<BigValue 
  data={stats} 
  value=avg_char_per_article
  title="Characters per article (avg.)"
  fmt=num1k
/>


```sql article_dates
  select
      month_date,n_articles
  from files.articles_dates
```

<BarChart
    data={article_dates}
    title="Last modification date"
    x=month_date
    y=n_articles
    yAxisTitle="articles"
/>




# Movies in Wikipedia ?


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

# Skyscrapers ?

Some stats about skyscrapers heights around the world.




<BigValue 
  data={stats} 
  value=skyscrapers_total
  title="Skyscrapers found"
  fmt=id
/>

```sql skyscrapers
  select
    country,number_of_skyscrapers,avg_max_height,max_height
  from files.skyscrapers
  order by max_height desc
```

<BarChart
    data={skyscrapers}
    title="Skyscrapers around the world"
    x=country
    y2=number_of_skyscrapers
    y=max_height
    type=grouped
    yAxisTitle="meters"
    y2AxisTitle="buildings"
/>


# Population divisions

```sql countries_pop
  select
      country,
      case 
        when country=='France' then concat(country, ' 🇫🇷')
        when country=='Poland' then concat(country, ' 🇵🇱')
        when country=='USA' then concat(country, ' 🇺🇸')
      else country end as abbrev
  from files.pop_countries
  group by country
```

<Dropdown data={countries_pop} name=country_pop value=country label=abbrev >
    <DropdownOption value="%" valueLabel="All Coutries"/>
</Dropdown>


```sql population_totals
with selection as (
    select 
        country,
        population,
        n_cities,
        country LIKE '${inputs.country_pop.value}' as in_selection
    from pop_countries
)

select 
    --in_selection,
    sum(population) as pop_total,
    sum(n_cities) as cities_total
from selection
where in_selection
group by in_selection

```

<BigValue 
  data={population_totals} 
  value=pop_total
  title="Selected population"
  fmt=num2m
/>

<BigValue 
  data={population_totals} 
  value=cities_total
  title="Selected cities"
  fmt=num1k
/>


```sql population
  select
    country,
    division,
    population,n_cities,
    population/n_cities as pop_per_city
  from files.pop_countries
   where country LIKE '${inputs.country_pop.value}'
```

<BarChart
    data={population}
    title="Most populated divisions in {inputs.country_pop.label}"
    x=division
    y=pop_per_city
    y2=population
    type=grouped
    yAxisTitle="inhabitants"
    y2AxisTitle="cities"
/>
