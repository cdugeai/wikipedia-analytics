---
title: Some Wikipedia Stats
---

# What is this page ?

This page is presenting a work of **data engineering** more than data analytics. However, as it is not easy to showcase data engineering work, I wanted to present the result of the tasks in a more friendly format.


<Details title="Click here for full context, data and tools">


So this document is **not a data analytics showcase** but an *evidence* that **large analytics and data exploration don't necessarily require expensive clusters** of machines:

- the whole wikipedia can be processed in 30s on a single machine
- data exploration later in this page fully runs in the browser, without server





## What data ?

The dataset is an dump containing wikipedia articles. I chose this dataset as a subset of reasonable size has already been created: **a dump of all French articles of Wikipedia (34GB)**.
Here is the official description of this dataset:

> This dataset contains all articles of the English and French language editions of Wikipedia, pre-parsed and outputted as structured JSON files with a consistent schema (JSONL compressed as zip). Each JSON line holds the content of one full Wikipedia article stripped of extra markdown and non-prose sections (references, etc.).
> 
> https://huggingface.co/datasets/wikimedia/structured-wikipedia#dataset-summary


|              | Dataset info                                                   |
|--------------|----------------------------------------------------------------|
| Content      | French Wikipedia                                               |
| Size         | 34GB                                                           |
| Date of dump | 16 September 2024                                              |
| Source       | https://huggingface.co/datasets/wikimedia/structured-wikipedia |
|              |                                                                |


## What tools ?

To query the dataset and perform the analytics calculations present in the document, I created a few queries in **Pyspark** and **Polars**. 


However, thanks to modern tools, we can now compute a large volume of data on a single machine, no matter the available memory. 
Here are the processing time to crawl the whole dataset for any query I wrote:

| Engine  | Processing time |
|---------|-----------------|
| Polars  | `< 50s`          |
| Pyspark | `< 30s`          |

<Grid cols=2>
    <Image 
        url="https://www.bigdatawire.com/wp-content/uploads/2024/10/Polars_logo_1.png"
        description="Sample placeholder image"
        height=200
    />
    <Image 
            url="https://images.seeklogo.com/logo-png/34/2/apache-spark-logo-png_seeklogo-349535.png"
            description="Sample placeholder image"
            height=200
    />
</Grid>



As the input data is a collection of `.jsonl` files, every query had to parse the whole 34GB of content. 
This is why the queries are relatively slow compared to `.parquet` or Arrow formats. Even CSV files would have been faster to process.

Read [this article](https://medium.com/@ManueleCaddeo/understanding-jsonl-bc8922129f5b) to know more about `.jsonl` format.

To create this page, I used [the awesome tool Evidence](https://evidence.dev/).


</Details>

# 📊 General statistics

## Articles and characters

```sql stats
  select
      n_articles, total_char, avg_char_per_article
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


So French Wikipedia contains **2.7** million articles, that represents **22 billion characters** ! This is equivalent of **reading Romeo and Juliet 150 000 times**.

<Image 
    url="https://media1.giphy.com/media/v1.Y2lkPTZjMDliOTUyZm1kdDQ2dncxMnNtcmo4ZjBuaDVzYTlld3FpcmkzOTU0OHhiYnQzZiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/WoWm8YzFQJg5i/giphy.webp"
    description="Sample placeholder image"
    height=200
/>


## Creation timeline

Here is a timeline of the creation of articles, every quarter from year 2001.

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



# ⚽️ Footballer stats

In this section, you can explore some statistics about football players present in the French Wikipedia.

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

## Players by country


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

## Right or left foot ?

Here is a plot displaying the ration of right-footed players compared to the total of players of the country. For most of the countries, the ratio is around 70%. 

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

# 🎬 Movies

## Genres and countries

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

## Duration by genre

```sql boxplot_films_by_genre
  select 
    UPPER(SUBSTRING(f.genre, 1, 1)) || SUBSTRING(f.genre, 2) as genre,
    f.n_movies,dur_q1,dur_q3,max_duration,min_duration,avg_duration,median_duration
  from 
    files.films_by_genre f
    right join ${movies_top_genres} g on g.genre=f.genre
  order by median_duration DESC
```




<Grid cols=2>
    <Image 
        url="https://c.tenor.com/gDWXh_83aQgAAAAd/tenor.gif"
        description="Sample placeholder image"
        height=200
    />
    <p>Are actions movies are actually longer than documentaries ? <br/>
    Here is the distribution of all movies durations among the selected genres.
    </p>
</Grid>



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

# 💫 Skyscrapers

Number of skyscrapers for each country, with the height of the tallest building.

```sql skyscrapers_top_countries
    with 
    c as (select country, sum(n_skyscrapers) as n_skyscrapers from files.skyscrapers group by country),
    top_c as (select country, n_skyscrapers from c order by n_skyscrapers DESC limit ${inputs.skyc_countries_limit})
    select country, n_skyscrapers from top_c
```


```sql skyscrapers_stats
    select sum(n_skyscrapers) as skyscrapers_total from ${skyscrapers_top_countries}
```



```sql skyscrapers
  select
    s.country,s.n_skyscrapers,s.max_height,s.min_height,s.avg_height
  from files.skyscrapers s
    right join ${skyscrapers_top_countries} top_s on top_s.country=s.country
  order by s.n_skyscrapers desc
```

<Grid col=2>
    <BigValue 
      data={skyscrapers_stats} 
      value=skyscrapers_total
      title="Skyscrapers in selection"
      fmt=id
    />
    <Slider
      title="Countries"
      name=skyc_countries_limit
      min=5
      max=300
      step=1
    />
    
</Grid>

<BarChart
    data={skyscrapers}
    title="Skyscrapers around the world"
    x=country
    y=n_skyscrapers
    y2=max_height
    type=grouped
    y2AxisTitle="meters"
    yAxisTitle="buildings"
/>


## Search the data

With this table, you can search, sort and export the data you need.

<DataTable data={skyscrapers} search=true rows=5 totalRow=true title="Search skyscrapers data" rowShading=false> 
    <Column id=country totalAgg="Selected countries"/>
    <Column id=n_skyscrapers totalAgg=sum contentType=colorscale colorScale=positive title="Skyscrapers"/>
    <Column id=max_height title="Height max (m)" totalAgg=mean weightCol=gdp_usd fmt='id' contentType=colorscale colorScale=positive/>
    <Column id=min_height title="Height min (m)" totalAgg=mean fmt='#,##0"m"'/>
    <Column id=avg_height title="Height avg (m)" totalAgg=mean fmt='#,##0"m"' contentType=colorscale colorScale=positive/>
</DataTable>

*Note:* Some Skyscrapers data can be related to projects that haven't been finished as it is the case for [this 750m tower in France](https://fr.wikipedia.org/wiki/Tour_Tourisme_TV).

# What next ?

<Details title="Next steps">

    Analyse the changes of Wikipedia in **real-time**. 
    
    For this, Wikipedia provides an [EventStreams HTTP Service](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service) all the events in real_time.
    The tools I will use for this task will be [Apache Kafka](https://kafka.apache.org/), [Apache Flink](https://flink.apache.org/) (and probably [Apache Beam](https://beam.apache.org/))
</Details>
