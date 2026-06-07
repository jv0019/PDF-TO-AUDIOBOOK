# 🎧 PDF to Audiobook Generator

A Python automation tool that converts PDF documents into high-quality audiobooks using Amazon Polly Text-to-Speech.

The application extracts text from PDF documents, intelligently splits content into manageable chunks, generates speech using AWS Polly, processes audio in parallel for improved performance, and combines all generated segments into a single MP3 audiobook.

Built with **Python**, **AWS Polly**, **pdfplumber**, and **pydub**, the tool transforms static documents into portable audio content suitable for learning, accessibility, and productivity.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![AWS](https://img.shields.io/badge/AWS-Polly-orange)
![Automation](https://img.shields.io/badge/Workflow-Automation-green)
![Accessibility](https://img.shields.io/badge/Accessibility-Audiobooks-red)

---

## 🚀 Overview

Many valuable resources exist exclusively in PDF format:

* Books
* Research papers
* Technical documentation
* Reports
* Study material
* Training manuals

Reading lengthy documents can be time-consuming and impractical during travel, exercise, or other activities.

This tool automatically converts PDF content into an audiobook that can be listened to on:

* Smartphones
* Tablets
* Laptops
* Smart speakers
* MP3 players

The result is a hands-free learning and accessibility solution.

---

## 🎯 Problem

Traditional PDF documents are not always convenient to consume.

Common challenges include:

* Limited time available for reading
* Difficulty reading long documents
* Accessibility barriers for visually impaired users
* Manual text-to-speech conversion being tedious and repetitive
* Large documents exceeding text-to-speech service limits

---

## 💡 Solution

The PDF to Audiobook Generator automates the entire workflow:

✅ Extracts text from PDFs

✅ Handles large documents automatically

✅ Splits content within AWS Polly limits

✅ Generates speech in parallel

✅ Merges audio segments seamlessly

✅ Produces a single audiobook file

✅ Cleans temporary files automatically

This allows users to convert lengthy documents into portable audio content within minutes.

---

## ✨ Features

### 📄 PDF Text Extraction

* Extracts content from PDF documents
* Processes multi-page files
* Supports books, reports, and research papers

### 🔊 Text-to-Speech Conversion

* Powered by Amazon Polly
* Natural-sounding speech synthesis
* Cloud-based voice generation

### ⚡ Parallel Processing

* Multi-threaded audio generation
* Faster conversion times
* Improved scalability for large documents

### ✂ Intelligent Text Chunking

* Automatically handles AWS Polly character limits
* Splits content into manageable segments
* Maintains processing reliability

### 🎵 Audio Merging

* Combines multiple audio files into a single audiobook
* Creates a seamless listening experience

### 🧹 Automatic Cleanup

* Removes temporary audio files
* Keeps project directories organized

---

## 🏗 Workflow

```text id="i53n2g"
PDF Document
      │
      ▼
Text Extraction
(pdfplumber)
      │
      ▼
Text Chunking
      │
      ▼
Parallel AWS Polly Requests
      │
      ▼
MP3 Segment Generation
      │
      ▼
Audio Merging
(pydub)
      │
      ▼
Final Audiobook
```

---

## 🛠 Technology Stack

| Component          | Technology        |
| ------------------ | ----------------- |
| Language           | Python            |
| PDF Processing     | pdfplumber        |
| Text-to-Speech     | AWS Polly (boto3) |
| Audio Processing   | pydub             |
| Parallel Execution | ThreadPool        |
| File Management    | OS Module         |
| Audio Encoding     | FFmpeg            |

---

## 📂 Project Structure

```text id="4wn9e6"
pdf-to-audiobook/
│
├── main.py
├── requirements.txt
└── README.md
```

---

## ⚙️ Installation

### Clone Repository

```bash id="eqyvn5"
git clone https://github.com/jv0019/pdf-to-audiobook.git
cd pdf-to-audiobook
```

### Install Dependencies

```bash id="42xkgu"
pip install -r requirements.txt
```

---

## 📦 Requirements

### Python Packages

```text id="6agxwp"
pdfplumber
boto3
pydub
```

### Additional Requirements

* AWS Account
* AWS Credentials
* FFmpeg Installation

---

## 🔑 AWS Configuration

Configure AWS credentials locally:

```bash id="p0s3rz"
aws configure
```

Provide:

```text id="4t18y7"
AWS Access Key ID
AWS Secret Access Key
Region
```

Or use environment variables:

```env id="8dyj4c"
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=your_region
```

---

## 🎵 Installing FFmpeg

### Windows

Download FFmpeg from:

```text id="0j1qrd"
https://ffmpeg.org/download.html
```

Add FFmpeg to your system PATH.

### Ubuntu

```bash id="20ivom"
sudo apt install ffmpeg
```

### macOS

```bash id="j6xgsa"
brew install ffmpeg
```

---

## ▶️ Usage

### Configure Input and Output Files

Update the file paths in the script:

```python
pdf_path = "path_to_your_pdf.pdf"
output_audio_file = "output.mp3"
```

### Run the Application

```bash id="m4ikzm"
python main.py
```

After processing completes, the generated audiobook will be available as:

```text id="a7g11l"
output.mp3
```

---

## 📈 Benefits

### Accessibility

* Supports visually impaired users
* Enables audio-first content consumption
* Improves document accessibility

### Productivity

* Learn while commuting
* Listen during workouts
* Consume content hands-free

### Automation

* Eliminates repetitive manual conversion
* Processes large documents automatically
* Handles text-to-speech limitations transparently

---

## 💼 Example Use Cases

### Education

* Convert textbooks into audiobooks
* Listen to study materials
* Review lecture notes

### Research

* Listen to research papers
* Consume technical documentation
* Review reports while multitasking

### Accessibility

* Create audio content for visually impaired users
* Improve document accessibility compliance

### Professional Development

* Convert training manuals
* Listen to industry reports
* Transform technical guides into audio learning resources

---

## 🚀 Future Enhancements

Planned improvements:

* [ ] Chapter detection and navigation
* [ ] Multiple voice selection
* [ ] Adjustable narration speed
* [ ] Graphical user interface (GUI)
* [ ] Web-based version
* [ ] EPUB support
* [ ] Speaker differentiation
* [ ] Audiobook metadata generation
* [ ] Background music support
* [ ] Cloud storage integration

---

## 📜 License

MIT License

---

## 👤 Author

**Jivitesh Sachdev**

Software Development • Automation • Cloud Applications • Accessibility Technology

GitHub: https://github.com/jv0019

---

### Keywords

Python • AWS Polly • Text-to-Speech • PDF Processing • Audiobook Generator • Accessibility Technology • Automation • Cloud Computing • Audio Processing • Productivity Tools
