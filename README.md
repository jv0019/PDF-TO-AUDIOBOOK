# PDF to Audiobook Generator

A Python automation tool that converts PDF documents into audiobooks using AWS Polly text-to-speech.

The script extracts text from a PDF, splits the text into manageable chunks, converts each chunk into speech using Amazon Polly, and combines the generated audio files into a single MP3 audiobook.

---

## Problem

Many long documents such as books, research papers, or reports exist only in PDF format. Reading them can be time-consuming, and converting them into audio manually is tedious.

This tool automates the process by transforming any PDF document into an audiobook that can be listened to on phones, computers, or audio players.

---

## Solution

The program performs the following steps automatically:

1. Extracts text from a PDF file.
2. Splits the extracted text into chunks within AWS Polly limits.
3. Converts each chunk into speech using AWS Polly.
4. Processes chunks in parallel to speed up generation.
5. Combines all generated audio segments into one MP3 file.
6. Cleans up temporary audio files.

The result is a complete audiobook generated from the original PDF.

---

## Features

- Extracts text from PDF documents
- Converts text to speech using AWS Polly
- Parallel processing for faster audio generation
- Automatic text chunking to handle API limits
- Combines multiple audio segments into a single audiobook file
- Automatic cleanup of temporary files

---

## Tech Stack

- **Python**
- **pdfplumber** – PDF text extraction
- **AWS Polly (boto3)** – Text-to-speech synthesis
- **pydub** – Audio processing and merging
- **ThreadPool** – Parallel audio generation
- **OS module** – File management

---

## How It Works

1. The script reads the PDF using `pdfplumber`.
2. All text is extracted page by page.
3. The text is split into chunks under AWS Polly's character limit.
4. Each chunk is sent to AWS Polly to generate speech.
5. Audio segments are temporarily saved as MP3 files.
6. All segments are merged into a single audiobook file.

---

## Installation

Clone the repository:

```bash
git clone https://github.com/yourusername/pdf-to-audiobook.git
cd pdf-to-audiobook
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Requirements

Create a `requirements.txt` file with the following:

```
pdfplumber
boto3
pydub
```

You will also need:

- An **AWS account**
- AWS credentials configured locally
- **FFmpeg installed** for audio processing (required by pydub)

Install FFmpeg:

Mac (Homebrew)

```
brew install ffmpeg
```

Ubuntu

```
sudo apt install ffmpeg
```

Windows

Download from:

https://ffmpeg.org/download.html

---

## Usage

Update the file paths in the script:

```python
pdf_path = "path_to_your_pdf.pdf"
output_audio_file = "output.mp3"
```

Run the script:

```bash
python main.py
```

After processing, the audiobook will be saved as:

```
output.mp3
```

---

## Example Use Cases

- Convert ebooks to audiobooks
- Listen to research papers or reports
- Create accessible content for visually impaired users
- Convert documentation into audio learning material

---

## Project Structure

```
pdf-to-audiobook
│
├── main.py
├── README.md
```

---

## Future Improvements

Possible improvements for future versions:

- Chapter detection
- GUI interface
- Multiple voice options
- Adjustable speech speed
- Background music support
- Web interface

