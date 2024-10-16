```sql

            select aa.state, sum(aa.total_population) state_population,
            sum(bb.violent) violence_arrest
            from `ids706_data_engineering`.`default`.`ar805_population_db` aa
            left join ids706_data_engineering.default.ar805_arrest_db bb
            on aa.county = bb.county
            group by aa.state
            having violence_arrest is not null
            order by violence_arrest desc;
            
```

```response from databricks
[Row(state='Vermont', state_population=72604, violence_arrest=6145), Row(state='Arizona', state_population=77330, violence_arrest=753), Row(state='California', state_population=196586, violence_arrest=642), Row(state='Iowa', state_population=383038, violence_arrest=503), Row(state='Tennessee', state_population=2408908, violence_arrest=349)]
```

```sql

            select aa.state, sum(aa.total_population) state_population,
            sum(bb.violent) violence_arrest
            from `ids706_data_engineering`.`default`.`ar805_population_db` aa
            left join ids706_data_engineering.default.ar805_arrest_db bb
            on aa.county = bb.county
            group by aa.state
            having violence_arrest is not null
            order by violence_arrest desc;
            
```

```response from databricks
[Row(state='California', state_population=12911020, violence_arrest=46108), Row(state='Florida', state_population=4502902, violence_arrest=6145), Row(state='North Carolina', state_population=2714903, violence_arrest=6145), Row(state='Idaho', state_population=196482, violence_arrest=641), Row(state='Iowa', state_population=382815, violence_arrest=503), Row(state='New York', state_population=4778864, violence_arrest=390), Row(state='Colorado', state_population=1478536, violence_arrest=349), Row(state='Michigan', state_population=3448703, violence_arrest=349), Row(state='Montana', state_population=62440, violence_arrest=349), Row(state='Texas', state_population=2158386, violence_arrest=65)]
```

