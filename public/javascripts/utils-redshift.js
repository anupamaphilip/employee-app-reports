var config = require("../config/config.js");

//connection to the redshift
var Redshift = require('node-redshift');
var client = {
  user: config.rs_user,
  password: config.rs_password,
  database: config.rs_database, 
  port: config.rs_port,
  host: config.rs_host
}; 
var redshiftClient = new Redshift(client);


//connection to local mysql
var mysql = require('mysql');
var con = mysql.createConnection({
      host: config.mysql_host,
      user: config.mysql_user,
      password: config.mysql_password,
      database: config.mysql_database,
      charset : config.mysql_charset
    });

module.exports = {

  data: function() {

    console.log("inside utils-redshift data");

    return new Promise(function(resolve, reject) 
    {

      con.connect(function (err) {

        console.log("connected to mysql table");

        //creating app installed table
        redshiftClient.query("Select uuid,id,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id from mob_emp_app_dev.application_installed where received_at >= (GETDATE() - interval '1 day')",function(err,data)
        {
          if(err) {
            console.log("error: " + err);
            throw err;
          }
          else
          {
            console.log("no error");
            var responseJson = JSON.stringify(data.rows); //stringifying the json data and storing it in variable
            var parsedJsonValue = JSON.parse(responseJson); //parsing the stringified values
            for(var i=0; i< parsedJsonValue.length; i++) 
            {
              var context = parsedJsonValue[i];
              if(err) throw err;
              else
              {
                var query = "INSERT IGNORE INTO application_installed_sql(uid,id,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,company_id)VALUES ('" + context.uuid + "', '" + context.id + "', '" + context.context_device_type + "', '" +  context.received_at + "', '" + context.user_id + "', '" + context.context_traits_first_name + "', '" + context.context_traits_last_name + "', '" + context.context_traits_company_id + "')"
                con.query(query);
              }
            }
            console.log("App Installed table has been updated with current value");
          }
        });

        // creating view document table
        redshiftClient.query("Select context_traits_email,uuid,id,document_id,document_url,heading,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id from mob_emp_app_dev.view_document where received_at >= (GETDATE() - interval '1 day')",function(err,data)
        {
          if(err)throw err;
          else
          {
                 var responseJson = JSON.stringify(data.rows);
             var parsedJsonValue = JSON.parse(responseJson);
             for(var i=0; i< parsedJsonValue.length; i++)
             {
                var context = parsedJsonValue[i];
                var header = context.heading;
                var url = context.document_url;
                var finalheader, finalurl;

                if(header == '' || header == null || url == "" || url == null) // to check if the record contains null or empty string
                {
                  finalheader = null;
                  finalurl = null;
                }
                else
                {
                  finalheader = header.replace(/'/g, "''"); // replace the header column which has a single quote with double single quotes
                  finalurl = url.replace(/'/g, "''");  // replace the url which has a single quotes
                }
                var query = "INSERT IGNORE INTO view_document_sql(context_traits_email,uid,id,document_id,document_url,document_title,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id) VALUES ('" + context.context_traits_email + "', '" + context.uuid + "', '" + context.id + "', '" + context.document_id + "', '" + finalurl + "', '" + finalheader + "', '" +  context.context_device_type + "', '" + context.received_at + "', '" + context.user_id + "', '" + context.context_traits_first_name + "', '" + context.context_traits_last_name + "', '" + context.context_traits_company_id + "')"
                con.query(query);         
             }
              console.log("View document table has been updated with current value");

          }
        }); 

        // creating shared document table
        redshiftClient.query("Select context_traits_email,uuid,id,document_id,document_url,heading,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id from mob_emp_app_dev.share_document where received_at >= (GETDATE() - interval '1 day')",function(err,data)
        {
          if(err)throw err;
          else
          {
                 var responseJson = JSON.stringify(data.rows);
             var parsedJsonValue = JSON.parse(responseJson);
             for(var i=0; i< parsedJsonValue.length; i++)
             {
                var context = parsedJsonValue[i];
                var header = context.heading;
                var url = context.document_url;
                var finalheader, finalurl;

                if(header == '' || header == null || url == "" || url == null)
                {
                  finalheader = null;
                  finalurl = null;
                }
                else
                {
                  finalheader = header.replace(/'/g, "''");
                  finalurl = url.replace(/'/g, "''"); 
                }
                var query = "INSERT IGNORE INTO share_document_sql(context_traits_email,uid,id,document_id,document_url,document_title,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id) VALUES ('" + context.context_traits_email + "', '" + context.uuid + "', '" + context.id + "', '" + context.document_id + "', '" + finalurl + "', '" + finalheader + "', '" +  context.context_device_type + "', '" + context.received_at + "', '" + context.user_id + "', '" + context.context_traits_first_name + "', '" + context.context_traits_last_name + "', '" + context.context_traits_company_id + "')"
                con.query(query);         
             }
              console.log("Shared document table has been updated with current value");

          }
        }); 

        // creating comment document table
        redshiftClient.query("Select context_traits_email,uuid,id,document_id,document_url,heading,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id from mob_emp_app_dev.comment_on_document where received_at >= (GETDATE() - interval '1 day')",function(err,data)
        {
          if(err)throw err;
          else
          {
                 var responseJson = JSON.stringify(data.rows);
             var parsedJsonValue = JSON.parse(responseJson);
             for(var i=0; i< parsedJsonValue.length; i++)
             {
                var context = parsedJsonValue[i];
                var header = context.heading;
                var url = context.document_url;
                var finalheader, finalurl;

                if(header == '' || header == null || url == "" || url == null)
                {
                  finalheader = null;
                  finalurl = null;
                }
                else
                {
                  finalheader = header.replace(/'/g, "''");
                  finalurl = url.replace(/'/g, "''"); 
                }
                var query = "INSERT IGNORE INTO comment_on_document_sql(context_traits_email,uid,id,document_id,document_url,document_title,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id) VALUES ('" + context.context_traits_email + "', '" + context.uuid + "', '" + context.id + "', '" + context.document_id + "', '" + finalurl + "', '" + finalheader + "', '" +  context.context_device_type + "', '" + context.received_at + "', '" + context.user_id + "', '" + context.context_traits_first_name + "', '" + context.context_traits_last_name + "', '" + context.context_traits_company_id + "')"
                con.query(query); 
             }
              console.log("comment_on_document table has been updated with current value");       

          }
        });

        // creating saved document table
        redshiftClient.query("Select context_traits_email,uuid,id,document_id,document_url,heading,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id,is_saved from mob_emp_app_dev.saved_document where received_at >= (GETDATE() - interval '1 day')",function(err,data)
        {
          if(err)throw err;
          else
          {
            var responseJson = JSON.stringify(data.rows);
            var parsedJsonValue = JSON.parse(responseJson);
             for(var i=0; i< parsedJsonValue.length; i++)
             {
                var context = parsedJsonValue[i];
                var header = context.heading;
                var url = context.document_url;
                var finalheader, finalurl;

                if(header == '' || header == null || url == "" || url == null)
                {
                  finalheader = null;
                  finalurl = null;
                }
                else
                {
                  finalheader = header.replace(/'/g, "''");
                  finalurl = url.replace(/'/g, "''"); 
                }
                var query = "INSERT IGNORE INTO saved_document_sql(context_traits_email,uid,id,document_id,document_url,document_title,context_device_type,received_at,user_id,context_traits_first_name,context_traits_last_name,context_traits_company_id,is_saved) VALUES ('" + context.context_traits_email + "', '" + context.uuid + "', '" + context.id + "', '" + context.document_id + "', '" + finalurl + "', '" + finalheader + "', '" +  context.context_device_type + "', '" + context.received_at + "', '" + context.user_id + "', '" + context.context_traits_first_name + "', '" + context.context_traits_last_name + "', '" + context.context_traits_company_id + "', '" + context.is_saved +"')"
                con.query(query);         
             }
              console.log("saved_document table has been updated with current value");
          }
        });   

        // for foreground table
        redshiftClient.query("Select uuid,event,event_text,context_traits_email,received_at,user_id,context_traits_company_id,context_traits_first_name,context_traits_last_name,context_device_type from mob_emp_app_dev.foreground where received_at >= (GETDATE() - interval '1 day')",function(err,data)
        {
          if(err)throw err;
          else
          {
            var responseJson = JSON.stringify(data.rows);
            var parsedJsonValue = JSON.parse(responseJson);
            for(var i=0; i< parsedJsonValue.length; i++)
            {
              var context = parsedJsonValue[i];
              var query = "INSERT IGNORE INTO foreground_sql(uid,event_session,event_text,context_traits_email,received_at,user_id,context_traits_company_id,context_traits_first_name,context_traits_last_name,context_device_type) VALUES ('" + context.uuid + "', '" + context.event + "', '" + context.event_text + "', '" + context.context_traits_email + "', '" + context.received_at + "', '" + context.user_id + "', '" + context.context_traits_company_id + "', '" + context.context_traits_first_name + "', '" + context.context_traits_last_name + "', '" + context.context_device_type +"')"
              con.query(query);                                                                                                                                                                                                                                                                                                                                      
            }
            console.log("foreground table has been updated with current value");
          }
        });   

        // for users table
        redshiftClient.query("Select uuid,received_at,context_traits_company_id,context_device_type,context_traits_email,context_traits_user_id,context_traits_first_name,context_traits_last_name from mob_emp_app_dev.users where received_at >= (GETDATE() - interval '1 day')",function(err,data)
        {           
          if(err)throw err;
          else
          {
                 var responseJson = JSON.stringify(data.rows);
             var parsedJsonValue = JSON.parse(responseJson);
             for(var i=0; i< parsedJsonValue.length; i++)
             {
                var context = parsedJsonValue[i];
                var query = "INSERT IGNORE INTO users_sql(uid,received_at,context_traits_company_id,context_device_type,context_traits_email,context_traits_user_id,context_traits_first_name,context_traits_last_name) VALUES ('" + context.uuid + "', '" + context.received_at + "', '" + context.context_traits_company_id + "', '" + context.context_device_type + "', '" + context.context_traits_email + "', '" + context.context_traits_user_id + "', '" + context.context_traits_first_name + "', '" + context.context_traits_last_name +"')"
                con.query(query);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
             }
              console.log("users table has been updated with current value");
          }
        });   

      });
    
    });

  }
};
