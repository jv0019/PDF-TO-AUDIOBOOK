import pdfplumber
import boto3
import os
from pydub import AudioSegment
from multiprocessing.dummy import Pool as ThreadPool

# Extract text from PDF
def extract_text_from_pdf(pdf_path):
    text = ''
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    return text

# Convert text chunk to speech
def text_to_speech_chunk(chunk, index, voice_id='Joanna'):
    polly_client = boto3.client('polly')
    response = polly_client.synthesize_speech(
        VoiceId=voice_id,
        OutputFormat='mp3',
        Text=chunk
    )
    output_file = f'temp_audio_{index}.mp3'
    with open(output_file, 'wb') as file:
        file.write(response['AudioStream'].read())
    return output_file

# Split text into chunks within Polly's limit
def split_text(text, max_chars=3000):
    words = text.split()
    chunks = []
    chunk = ""
    for word in words:
        if len(chunk) + len(word) + 1 > max_chars:
            chunks.append(chunk)
            chunk = word
        else:
            chunk += " " + word
    if chunk:
        chunks.append(chunk)
    return chunks

# Combine multiple audio files into one
def combine_audio_files(audio_files, output_file):
    combined = AudioSegment.empty()
    for file in audio_files:
        audio = AudioSegment.from_mp3(file)
        combined += audio
    combined.export(output_file, format='mp3')

# Main script
def pdf_to_audiobook(pdf_path, output_audio_file, voice_id='Joanna'):
    # Step 1: Extract text from PDF
    text = extract_text_from_pdf(pdf_path)
    
    # Step 2: Split text into chunks
    text_chunks = split_text(text)

    # Step 3: Convert text chunks to speech in parallel
    pool = ThreadPool(4)  # Number of threads
    audio_files = pool.starmap(text_to_speech_chunk, [(chunk, i, voice_id) for i, chunk in enumerate(text_chunks)])
    pool.close()
    pool.join()

    # Step 4: Combine audio files
    combine_audio_files(audio_files, output_audio_file)

    # Cleanup temporary audio files
    for file in audio_files:
        os.remove(file)

if __name__ == "__main__":
    pdf_path = 'path_to_your_pdf.pdf'  # Replace with your PDF file path
    output_audio_file = 'output.mp3'  # Replace with your desired output file name
    pdf_to_audiobook(pdf_path, output_audio_file)
    print(f'Audio content written to file "{output_audio_file}"')
