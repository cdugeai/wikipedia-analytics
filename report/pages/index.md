---
title: Some Wikipedia Stats
---

# Some stats

```sql stats
  select
      n_articles, total_char, avg_char_per_article, 11 as skyscrapers_total
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
      q_created,n_articles
  from files.articles_dates
  -- remove data with no creation date
  where len(q_created)>0
```

<BarChart
    data={article_dates}
    title="New articles per quarter"
    x=q_created
    y=n_articles
    yAxisTitle="articles"
    sort=false
    labels=false
    xAxisTitle="Date"
/>



# Footballer stats

[//]: # (Data selectors for football)

```sql countries_football
  select
      country_ref
  from files.footballer_stats
  group by country_ref
```

<Grid cols=1>
<div class="flex gap-4">
    <Dropdown data={countries_football} name=country_foot value=country_ref >
        <DropdownOption value="%" valueLabel="All Countries"/>
    </Dropdown>
    <Dropdown name=foot >
        <DropdownOption valueLabel="Any foot" value="%"/>
        <DropdownOption valueLabel="Left" value="left" />
        <DropdownOption valueLabel="Right" value="right" />
        <DropdownOption valueLabel="Both" value="both" />
    </Dropdown>
     <Slider
          title="Countries"
          name=football_countries_limit
          min=5
          max=300
          step=1
        />
</div>
   

</Grid>

<BigValue 
      data={stats_football} 
      value=n_players
      title="Players in selection"
      fmt=id
    />



[//]: # (Computed selected data for football)

```sql football_right_vs_left
with src as (select * from files.footballer_stats where foot in ('left', 'right'))
select country_ref,
    100* (sum(n_players) filter (where foot='right')) / (sum(n_players) filter (where foot='right')+sum(n_players) filter (where foot='left')) as pct_right
    --sum(n_players) filter (where foot='right') as t_right,
    --sum(n_players) filter (where foot='left') as t_left
from src
group by country_ref
order by country_ref
```

```sql football_selected_data
  select
    a.country_ref,
    a.foot,
    a.n_players,avg_height, 
    b.pct_right
  from files.footballer_stats a
  left join ${football_right_vs_left} b on a.country_ref=b.country_ref
      where a.country_ref LIKE '${inputs.country_foot.value}'
          and a.foot LIKE '${inputs.foot.value}'
        order by a.n_players DESC, a.foot
        limit ${inputs.football_countries_limit}

```

```sql stats_football
  select
      sum(n_players) as n_players
  from ${football_selected_data}
```

[//]: # (Graphs for football)



<BarChart
    data={football_selected_data}
    colorPaletteBkp={['#cf0d06','#eb5752','#e88a87']}
    colorPalette={['#c40000','#df6242','#f39e84','#ffd8cb']}
    title="Total players"
    x=country_ref
    y=n_players
    series=foot
    yAxisTitle="players"
    sort=false
/>

<ScatterPlot
    data={football_selected_data}
    colorPaletteBkp={['#cf0d06','#eb5752','#e88a87']}
    colorPalette={['#c40000','#df6242','#f39e84','#ffd8cb']}
    title="Percentage of right-footed players"
    x=country_ref
    y=pct_right
    yAxisTitle="%"
    sort=false
/>

# Movies in Wikipedia ?


```sql countries
  select
      country
  from files.films
  group by country
```

```sql movies_top_genres
    with 
    g as (select genre, sum(n_movies) as n_movies from files.films group by genre),
    top_g as (select genre, n_movies from g order by n_movies DESC limit 10)
    select genre, n_movies from top_g limit ${inputs.movies_genres_limit}
```

You can explore the movies present in Wikipedia. Country, genres and durations are explored in the following graphs.

<Grid cols=1>
<div class="flex gap-4">
    <Dropdown data={countries} name=country value=country >
    <DropdownOption value="%" valueLabel="All Countries"/>
    </Dropdown>
    <Dropdown data={movies_top_genres} name=genre value=genre>
        <DropdownOption value="%" valueLabel="All Genres"/>
    </Dropdown>
    <Slider
      title="Countries"
      name=movies_countries_limit
      min=5
      max=300
      step=1
    />
    <Slider
      title="Top genres"
      name=movies_genres_limit
      min=5
      max=10
      step=1
    />
</div>
</Grid>
<BigValue 
  data={total_movies} 
  value=n_movies
  title="Movies in selection"
  fmt=id
/>

```sql movies_top_countries
    with 
    c as (select country, sum(n_movies) as n_movies from files.films group by country),
    top_c as (select country, n_movies from c order by n_movies DESC limit ${inputs.movies_countries_limit})
    select country, n_movies from top_c
```

```sql movies_by_genre_country
  select 
  g.genre, f.country, f.avg_duration, f.n_movies
  from 
    files.films f
    right join ${movies_top_genres} g on g.genre=f.genre
    right join ${movies_top_countries} tc on tc.country=f.country
  where g.genre LIKE '${inputs.genre.value}'
```

```sql total_movies
  select sum(n_movies) as n_movies
  from ${movies_by_genre_country}
```

<BarChart
    data={movies_by_genre_country}
    title="Total movies, {inputs.country.label}"
    x=country
    y=n_movies
    series=genre
    colorPalette={["#003f5c","#2f4b7c","#665191","#a05195","#d45087","#f95d6a","#ff7c43","#ffa600"]}
    colorPaletteGreen={["#20982a","#40a23f","#58ad53","#6db767","#81c17a","#95cc8d","#a8d6a1","#bbe0b5","#ceeac9"]}
    yAxisTitle="movies"
/>

```sql boxplot_films_by_genre
  select 
    f.genre,f.n_movies,dur_q1,dur_q3,max_duration,min_duration,avg_duration,median_duration
  from 
    files.films_by_genre f
    right join ${movies_top_genres} g on g.genre=f.genre
  order by median_duration DESC
```

Here is the distribution of all movies durations among the selected genres.

<BoxPlot 
    data={boxplot_films_by_genre}
    title="Movie duration by genre"
    name=genre
    intervalBottom=dur_q1
    midpoint=median_duration
    intervalTop=dur_q3
    sort=false
    yAxisTitle="minutes"
    yFmt=id
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
    <DropdownOption value="%" valueLabel="All Countries"/>
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
