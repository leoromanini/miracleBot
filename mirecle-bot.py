import findspark
findspark.init('/opt/spark')
import schedule
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
import random
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import schedule
import time
from random import randrange
from datetime import date
from datetime import datetime


def get_spark_session():
    return SparkSession.builder.master('local[*]')\
        .config("spark.driver.memory", "12G").appName('EmailSender').getOrCreate()


def get_ingest_information():
    spark = get_spark_session()
    return spark.read.option('header', True).option('inferSchema', True)\
                     .option('delimiter', '|').csv('ingestor')


def get_avaliable_message(id_message=None):
    df = get_ingest_information()
    if id_message is None:
        messages_avaliable = df.filter(col('processado').isNull()).collect()
        return [random.choice(messages_avaliable)]
    else:
        return df.filter((col('processado').isNull()) & (col('id') == id_message)).collect()


def get_html_string(header, text):
    return """
       <!DOCTYPE HTML PUBLIC "-//W3C//DTD XHTML 1.0 Transitional //EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
       <html xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
       <head>
       <!--[if gte mso 9]>
       <xml>
         <o:OfficeDocumentSettings>
           <o:AllowPNG/>
           <o:PixelsPerInch>96</o:PixelsPerInch>
         </o:OfficeDocumentSettings>
       </xml>
       <![endif]-->
         <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
         <meta name="viewport" content="width=device-width, initial-scale=1.0">
         <meta name="x-apple-disable-message-reformatting">
         <!--[if !mso]><!--><meta http-equiv="X-UA-Compatible" content="IE=edge"><!--<![endif]-->
         <title></title>

           <style type="text/css">
             table, td {{ color: #000000; }} @media only screen and (min-width: 670px) {{
         .u-row {{
           width: 80% !important;
         }}
         .u-row .u-col {{
           vertical-align: top;
         }}

         .u-row .u-col-100 {{
           width: 80% !important;
         }}

       }}

       @media (max-width: 670px) {{
         .u-row-container {{
           max-width: 100% !important;
           padding-left: 0px !important;
           padding-right: 0px !important;
         }}
         .u-row .u-col {{
           min-width: 320px !important;
           max-width: 100% !important;
           display: block !important;
         }}
         .u-row {{
           width: calc(100% - 40px) !important;
         }}
         .u-col {{
           width: 100% !important;
         }}
         .u-col > div {{
           margin: 0 auto;
         }}
       }}
       body {{
         margin: 0;
         padding: 0;
       }}

       table,
       tr,
       td {{
         vertical-align: top;
         border-collapse: collapse;
       }}

       p {{
         margin: 0;
       }}

       .ie-container table,
       .mso-container table {{
         table-layout: fixed;
       }}

       * {{
         line-height: inherit;
       }}

       a[x-apple-data-detectors='true'] {{
         color: inherit !important;
         text-decoration: none !important;
       }}

       </style>



       <!--[if !mso]><!--><link href="https://fonts.googleapis.com/css?family=Lato:400,700&display=swap" rel="stylesheet" type="text/css"><link href="https://fonts.googleapis.com/css?family=Playfair+Display:400,700&display=swap" rel="stylesheet" type="text/css"><!--<![endif]-->

       </head>

       <body class="clean-body" style="margin: 0;padding: 0;-webkit-text-size-adjust: 100%;background-color: #f9f9f9;color: #000000">
         <!--[if IE]><div class="ie-container"><![endif]-->
         <!--[if mso]><div class="mso-container"><![endif]-->
         <table style="border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;min-width: 320px;Margin: 0 auto;background-color: #f9f9f9;width:100%" cellpadding="0" cellspacing="0">
         <tbody>
         <tr style="vertical-align: top">
           <td style="word-break: break-word;border-collapse: collapse !important;vertical-align: top">
           <!--[if (mso)|(IE)]><table width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td align="center" style="background-color: #f9f9f9;"><![endif]-->


       <div class="u-row-container" style="padding: 0px;background-color: transparent">
         <div class="u-row" style="Margin: 0 auto;min-width: 320px;max-width: 80%;overflow-wrap: break-word;word-wrap: break-word;word-break: break-word;background-color: #ffffff;">
           <div style="border-collapse: collapse;display: table;width: 100%;background-color: transparent;">
             <!--[if (mso)|(IE)]><table width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td style="padding: 0px;background-color: transparent;" align="center"><table cellpadding="0" cellspacing="0" border="0" style="width:80%;"><tr style="background-color: #ffffff;"><![endif]-->

       <!--[if (mso)|(IE)]><td align="center" width="80%" style="width: 80%;padding: 0px;border-top: 0px solid transparent;border-left: 0px solid transparent;border-right: 0px solid transparent;border-bottom: 0px solid transparent;" valign="top"><![endif]-->
       <div class="u-col u-col-100" style="max-width: 320px;min-width: 80%;display: table-cell;vertical-align: top;">
         <div style="width: 100% !important;">
         <!--[if (!mso)&(!IE)]><!--><div style="padding: 0px;border-top: 0px solid transparent;border-left: 0px solid transparent;border-right: 0px solid transparent;border-bottom: 0px solid transparent;"><!--<![endif]-->

       <table style="font-family:tahoma,arial,helvetica,sans-serif;" role="presentation" cellpadding="0" cellspacing="0" width="100%" border="0">
         <tbody>
           <tr>
             <td style="overflow-wrap:break-word;word-break:break-word;padding:30px 10px 10px;font-family:tahoma,arial,helvetica,sans-serif;" align="left">

         <div style="color: #333333; line-height: 140%; text-align: left; word-wrap: break-word;">
           <p style="font-size: 14px; line-height: 140%; text-align: center;"><span style="font-size: 28px; line-height: 39.2px; font-family: 'Playfair Display', serif; color: #000000;">{0}</span></p>
         </div>

             </td>
           </tr>
         </tbody>
       </table>

       <table style="font-family:tahoma,arial,helvetica,sans-serif;" role="presentation" cellpadding="0" cellspacing="0" width="100%" border="0">
         <tbody>
           <tr>
             <td style="overflow-wrap:break-word;word-break:break-word;padding:10px;font-family:tahoma,arial,helvetica,sans-serif;" align="left">

         <table height="0px" align="center" border="0" cellpadding="0" cellspacing="0" width="15%" style="border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;border-top: 3px solid #ff0009;-ms-text-size-adjust: 100%;-webkit-text-size-adjust: 100%">
           <tbody>
             <tr style="vertical-align: top">
               <td style="word-break: break-word;border-collapse: collapse !important;vertical-align: top;font-size: 0px;line-height: 0px;mso-line-height-rule: exactly;-ms-text-size-adjust: 100%;-webkit-text-size-adjust: 100%">
                 <span>&#160;</span>
               </td>
             </tr>
           </tbody>
         </table>

             </td>
           </tr>
         </tbody>
       </table>

       <table style="font-family:tahoma,arial,helvetica,sans-serif;" role="presentation" cellpadding="0" cellspacing="0" width="100%" border="0">
         <tbody>
           <tr>
             <td style="overflow-wrap:break-word;word-break:break-word;padding:15px 30px 25px;font-family:tahoma,arial,helvetica,sans-serif;" align="left">

         <div style="line-height: 150%; text-align: center; word-wrap: break-word;">
           <p style="font-size: 14px; line-height: 150%; text-align: center;"><span style="font-size: 16px; line-height: 24px; color: #555555; font-family: Lato, sans-serif;">{1}</span></p>
         </div>
         <br>
         <div style="line-height: 150%; text-align: center; word-wrap: break-word;">
           <p style="font-size: 14px; line-height: 150%; text-align: center;"><span style="font-size: 11px; line-height: 24px; color: #555555; font-family: Lato, sans-serif;">Miracle Bot ©</span></p>
         </div>

             </td>
           </tr>
         </tbody>
       </table>

         <!--[if (!mso)&(!IE)]><!--></div><!--<![endif]-->
         </div>
       </div>
       <!--[if (mso)|(IE)]></td><![endif]-->
             <!--[if (mso)|(IE)]></tr></table></td></tr></table><![endif]-->
           </div>
         </div>
       </div>


           <!--[if (mso)|(IE)]></td></tr></table><![endif]-->
           </td>
         </tr>
         </tbody>
         </table>
         <!--[if mso]></div><![endif]-->
         <!--[if IE]></div><![endif]-->
       </body>

       </html>

       """.format(header, text)
    
    
def send_email(_is_first_message):
    context = ssl.create_default_context()
    sender_email = "Miracle Bot"
    receiver_email = "receiver@gmail.com"

    if _is_first_message:
        message_info = get_avaliable_message(1)
    else:
        message_info = get_avaliable_message()

    if len(message_info) > 0:
        message = MIMEMultipart("alternative")
        message["Subject"] = message_info[0].assunto
        message["From"] = sender_email
        message["To"] = receiver_email

        html = get_html_string(message_info[0].titulo, message_info[0].mensagem)
        part2 = MIMEText(html, "html")
        message.attach(part2)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login("", "")
            server.sendmail(sender_email, receiver_email, message.as_string())

        mark_message_as_send(message_info[0].id)


def mark_message_as_send(id_message):
    df = get_ingest_information()
    df = df.cache()
    df_processed = df.filter(col('id') == id_message)
    df_processed = df_processed.withColumn('processado', lit(1))

    df = df.filter(col('id') != id_message)
    df = df.union(df_processed)
    df.count()

    df.coalesce(1).write.mode('overwrite').option("header", "true").option("delimiter", "|").csv('ingestor')
    df.unpersist()


if __name__ == '__main__':
    messages_avaliable_count = get_ingest_information().filter(col('processado').isNull()).count()
    if messages_avaliable_count > 0:
        message_day_and_hour = []

        message_days = random.sample(range(date.today().day+2, 30), messages_avaliable_count)
        message_hours = [random.choice(range(5, 23)) for i in range(messages_avaliable_count)]

        #Test
        message_hours.pop()
        message_hours.pop()
        message_hours.pop()
        message_hours.pop()
        message_days.pop()
        message_days.pop()
        message_days.pop()
        message_days.pop()

        message_days.append(3)
        message_hours.append(18)
        message_days.append(3)
        message_hours.append(19)
        message_days.append(3)
        message_hours.append(20)
        message_days.append(3)
        message_hours.append(21)


        #Initial message
        message_days[0] = 4
        message_hours[0] = 0
        is_first_message = True

        while True:
            now = datetime.now()
            for index, day in enumerate(message_days):
                if now.day == day and now.hour == message_hours[index]:
                    send_email(is_first_message)
                    message_days.remove(day)
                    message_hours.pop(index)
                    is_first_message = False

            time.sleep(30)
            if len(message_days) == 0:
                break
