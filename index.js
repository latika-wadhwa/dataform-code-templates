/**
 * Builds a type-2 slowly changing dimensions table and view.
 */
module.exports = (
    name,
    { uniqueKey, timestamp, source, tags, incrementalConfig }
  ) => {
  
    // Create a view on top of the raw updates table that contains computed valid_from and valid_to fields
  
    // Create an incremental table with just pure updates, for a full history of the table.
    const updates = publish(`${name}_updates`, {
      type: "incremental",
      tags,
      ...incrementalConfig,
    }).query(
      (ctx) => `
        select *
        from ${ctx.ref(source)}
        ${ctx.when(
          ctx.incremental(),
          `where ${timestamp} > coalesce((select max(${timestamp}) from ${ctx.self()}),'2000-01-01')`
        )}
  `
    );
  
    const max_date = publish(`max_data`, {
      type: "view",
      tags,
      columns: {
        max_date: `Second Max processed date of the Staging Table`,
      },
    }).query(
      (ctx) => `
    select
        coalesce(max(${timestamp}),"2000-01-01") as max_updated_at,
    from
      ${ctx.ref(`${name}_updates`)}
      where ${timestamp} <> (select max(${timestamp}) from ${ctx.ref(`${name}_updates`)})
  
    `
    );
  
  
  
  
  
  //     // Create a view on top of the raw updates table that contains computed eff_Date and exp_date fields.
  //   config {
  //     type: "operations"
  //   }
     
  
  
  // MERGE ${ref("source_data_scd_final")} as T
  // USING (Select * from ${ref("source_data_scd_updates")} where updated_at > (select max_updated_at	 from ${ref("max_data")} )) S
  // ON T.user_id = S.user_id
  // 	AND T.changing_field1 = S.changing_field1  
  //   	AND T.changing_field2= S.changing_field2
  //   	AND T.changing_field3 = S.changing_field3  
  // 	AND T.exp_date IS NULL  
  // WHEN NOT MATCHED BY SOURCE AND t.exp_date is null THEN
  // UPDATE 
  // SET t.exp_date = 
  // 		case 
  // 			when t.exp_date is NULL then
  // 		(select max(updated_at) from ${ref("source_data_scd_updates")})
  // 			else t.exp_date 
  // 		end   
  // WHEN NOT MATCHED BY TARGET THEN
  //   INSERT(	
  // user_id, changing_field1, changing_field2, changing_field3,updated_at, eff_date,exp_date)
  //   VALUES(user_id, changing_field1, changing_field2,changing_field3,updated_at,updated_at,null)
  
  
  
  //   `
  
  //   //  where row_num = (select max(row_num) from ${ctx.ref(`${name}`)} where ${uniqueKey} = out.${uniqueKey} and some_field = out.some_field)
  
  
  
  //   );
  
    // Returns the tables so they can be customized. where (${timestamp} = date_add(${timestamp}, interval 1 day) or ${timestamp} = date_add(${timestamp}, interval 0 day)) and (some_field <> ag_some_field or ag_some_field is null)
    return {  updates };
  };