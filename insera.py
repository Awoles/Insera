import nest_asyncio
import asyncio
import schedule
import time
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
import pandas as pd
import requests
from io import BytesIO
import os
from fastapi import FastAPI, Response
import uvicorn

app = FastAPI()

# Define a simple route for the root URL
@app.get("/")
async def read_root():
    return {"message": "Service is running"}

# Apply nest_asyncio to enable running asyncio loop within Colab
nest_asyncio.apply()

# Load the initial dataset from the Google Drive link
url = 'https://docs.google.com/uc?export=download&id=1FIWfHAykZlfR5zhfhw65OdM6l8wxA6kr'
response = requests.get(url)
data = pd.read_excel(BytesIO(response.content), sheet_name='Sheet1')
# Parse the dates with dayfirst=True
data['Reported_Date'] = pd.to_datetime(data['Reported_Date'], dayfirst=True, errors='coerce')
print(data.columns)  # Print column names for debugging

# Define the /start command handler
async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text(
        'Halo ges! Silahkan gunakan perintah /cekcek beserta No Tiket atau No Internet. Cth. /cekcek INC2121212'
    )
    print("Perintah /Start diterima")

# Define the /cekcek command handler
async def cekcek(update: Update, context: CallbackContext) -> None:
    query = update.message.text.strip().replace('/cekcek', '').strip()
    query = " ".join(query.split())  # Remove extra spaces
    
    if not query:  # Check if the query is empty
        response = "Data tidak ditemukan nih..\n\n"
        await update.message.reply_text(response)
        print("Empty query received, returned 'not found' message")
        return
    
    print(f"Query received: {query}")

    # Search by Incident or Service_No
    result = data[data['Incident'].str.contains(query, case=False, na=False)]
    
    if result.empty:
        result = data[data['Service_No'].astype(str).str.contains(query, case=False, na=False)]
    
    # Get overall date range from the entire dataset
    overall_earliest_date = data['Reported_Date'].min()
    overall_latest_date = data['Reported_Date'].max()
    date_range = f"Data tiket dari tgl:\n{overall_earliest_date.date()} s/d {overall_latest_date.date()}\n\n"
    
    if not result.empty:
        # Select specific columns
        limited_result = result[['Incident', 'Customer_Segment', 'Workzone', 'Ket_Gaul', 'Compliance', 'Service_No', 'Reported_Date', 'TTR_Customer', 'Jenis_Ggn']].head(4)
        
        # Format the response with the headline
        response = date_range

        for _, row in limited_result.iterrows():
            response += f"{row['Incident']} {row['Workzone']}\n{row['Ket_Gaul']} {row['Compliance']}\n{row['Service_No']} {row['Customer_Segment']}\n{row['Reported_Date']}\nTTR: {row['TTR_Customer']} Jam {row['Jenis_Ggn']}\n\n"

        # Add the summary
        summary = result['Summary'].iloc[0]  # Assuming you want to show the summary of the first matching row
        response += f"{summary}"
    else:
        response = f"Data tidak ditemukan nih..\n\n{date_range}"

    # Format the response with Markdown code block to enable easy copy
    formatted_response = f"```\n{response}\n```"

    await update.message.reply_text(formatted_response, parse_mode='Markdown')
    print(f"Response sent: {formatted_response}")

# Define the /kenkenganteng file upload handler
async def kenkenganteng(update: Update, context: CallbackContext) -> None:
    file = update.message.document
    if file.mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        file_id = file.file_id
        new_file = await context.bot.get_file(file_id)
        file_content = requests.get(new_file.file_path).content
        new_data = pd.read_excel(BytesIO(file_content), sheet_name='Sheet1')
        new_data['Reported_Date'] = pd.to_datetime(new_data['Reported_Date'], dayfirst=True, errors='coerce')

        # Combine the new data with the existing dataset and remove duplicates based on Incident and Reported_Date
        global data
        data = pd.concat([data, new_data], ignore_index=True).drop_duplicates(subset=['Incident', 'Reported_Date'])
        
        await update.message.reply_text("Data diterima dan diperbaharui, cihuy!")
        print("Data diterima dan diperbaharui")
    else:
        await update.message.reply_text("Formatnya salah kocak!!")

# Define the /kenkendownload command handler restricted to a specific chat ID
async def kenkendownload(update: Update, context: CallbackContext) -> None:
    chat_id = "247590309"
    if str(update.effective_chat.id) == chat_id:
        # Convert the updated data to Excel format
        with BytesIO() as buffer:
            data.to_excel(buffer, index=False, engine='xlsxwriter')
            buffer.seek(0)
            
            # Send the updated Excel file to the user
            await context.bot.send_document(chat_id=update.effective_chat.id, document=InputFile(buffer, filename='updated_data.xlsx'))
            await update.message.reply_text("Ini ges data terbarunya")
            print("Data dikirim ke pengguna")
    else:
        await update.message.reply_text("Eh! elu siapeh! cuman pembuat yang bisa kocak. Sungkem dulu gih!")

# Define the /DownloadinYaah command handler for all users
async def DownloadinYaah(update: Update, context: CallbackContext) -> None:
    # Convert the updated data to Excel format
    with BytesIO() as buffer:
        data.to_excel(buffer, index=False, engine='xlsxwriter')
        buffer.seek(0)
        
        # Send the updated Excel file to the user
        await context.bot.send_document(chat_id=update.effective_chat.id, document=InputFile(buffer, filename='updated_data.xlsx'))
        await update.message.reply_text("Ini ges data terbarunya")
        print("Data dikirim ke pengguna")

async def scheduled_download(application: Application, chat_id: str):
    context = CallbackContext(application)
    await kenkendownload(context, chat_id)

# Function to run the scheduled tasks
def run_schedule(application: Application, chat_id: str):
    schedule.every().day.at("00:00").do(asyncio.run, scheduled_download(application, chat_id))
    while True:
        schedule.run_pending()
        time.sleep(1)

# Main function to run the bot and the FastAPI server
async def main():
    # Replace 'YOUR_TELEGRAM_BOT_TOKEN' with your actual bot token
    token = "7916390846:AAF68w9Mr0yI9CUOMUTELTPesp_rJTb99k8"
    chat_id = "247590309"

    # Initialize the Application with the token
    application = Application.builder().token(token).build()

    # Register the command handler for /start
    application.add_handler(CommandHandler("start", start))

    # Register the command handler for /cekcek
    application.add_handler(CommandHandler("cekcek", cekcek))

    # Register the file upload handler for /kenkenganteng
    application.add_handler(MessageHandler(filters.Document.ALL, kenkenganteng))

    # Register the command handler for /kenkendownload
    application.add_handler(CommandHandler("kenkendownload", kenkendownload))

    # Register the command handler for /DownloadinYaah
    application.add_handler(CommandHandler("DownloadinYaah", DownloadinYaah))

    # Start the bot
    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    # Start the schedule for the specific chat ID
    asyncio.create_task(run_schedule(application, chat_id))

    # Run the FastAPI server
    port = int(os.environ.get('PORT', 50000))
    print(f"Starting server on port {port}")  # Debug print to ensure port is read correctly
    config = uvicorn.Config(app, host='0.0.0.0', port=port)
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == '__main__':
    nest_asyncio.apply()
    asyncio.run(main())
