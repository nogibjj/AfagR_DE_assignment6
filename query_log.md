```sql

           select aa.state, sum(aa.total_population) state_population,
            sum(bb.violent) violence_arrest, 
            round((violence_arrest/state_population)*100,1) as violence_percent
            from `ids706_data_engineering`.`default`.`ar805_population_db` aa
            left join ids706_data_engineering.default.ar805_arrest_db bb
            on aa.county = bb.county
            group by aa.state
            having violence_arrest is not null
            order by violence_arrest desc;
            
```

```response from databricks
[Row(state='California', state_population=7128866, violence_arrest=24989, violence_percent=0.4), Row(state='New York', state_population=4671111, violence_arrest=6145, violence_percent=0.1)]
```

