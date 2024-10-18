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
[Row(state='California', state_population=6403071, violence_arrest=21368, violence_percent=0.3), Row(state='Vermont', state_population=158125, violence_arrest=6145, violence_percent=3.9), Row(state='Virginia', state_population=2290260, violence_arrest=6145, violence_percent=0.3), Row(state='Illinois', state_population=2770610, violence_arrest=349, violence_percent=0.0), Row(state='Indiana', state_population=1072595, violence_arrest=349, violence_percent=0.0), Row(state='Colorado', state_population=323705, violence_arrest=349, violence_percent=0.1), Row(state='Arkansas', state_population=475032, violence_arrest=211, violence_percent=0.0)]
```

