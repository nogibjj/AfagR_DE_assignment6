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

