from flask import Flask, request, jsonify
import io
import os
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime
import uuid
from flask_cors import CORS
import requests
import time

app = Flask(__name__)
CORS(app)

# Configurações globais
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
ASSEMBLYAI_API_KEY = "0de8f335d2244fa185bae87577e5d449"  # Substitua pela sua chave
TEMP_FOLDER = "temp_processing"
UPLOAD_ENDPOINT = "https://api.assemblyai.com/v2/upload"
TRANSCRIPT_ENDPOINT = "https://api.assemblyai.com/v2/transcript"

# Garante que a pasta temporária existe
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Headers para requisições da AssemblyAI
headers = {
    "authorization": ASSEMBLYAI_API_KEY,
    "content-type": "application/json"
}

def authenticate_with_oauth2():
    """Autenticação com OAuth2"""
    flow = InstalledAppFlow.from_client_secrets_file(
        "credentials.json",
        SCOPES,
        redirect_uri="http://localhost:5000"
    )
    creds = flow.run_local_server(port=5000)
    return creds

def list_files_in_folder(service, folder_id):
    """Lista arquivos filtrando por extensão de áudio e pelo ID do projeto 822"""
    results = service.files().list(
        q=f"'{folder_id}' in parents and (name contains '.wav'  ) and name contains '_822_'",
        fields="files(id, name, mimeType, createdTime, modifiedTime)"
    ).execute()
    return results.get('files', [])

def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def upload_to_assemblyai(audio_file_path):
    """Faz upload do arquivo de áudio para a AssemblyAI"""
    def read_file(filename, chunk_size=5242880):
        with open(filename, 'rb') as _file:
            while True:
                data = _file.read(chunk_size)
                if not data:
                    break
                yield data
    
    upload_response = requests.post(
        UPLOAD_ENDPOINT,
        headers=headers,
        data=read_file(audio_file_path))
    
    if upload_response.status_code != 200:
        raise Exception(f"Upload failed: {upload_response.text}")
    
    return upload_response.json()['upload_url']

def transcribe_with_assemblyai(audio_url):
    """Transcreve usando AssemblyAI com diarização"""
    transcript_request = {
        'audio_url': audio_url,
        'language_code': 'pt',
        'speaker_labels': True,
        'speakers_expected': 2,  # Ajuste conforme o número esperado de falantes
        'punctuate': True,
        'format_text': True,
        'dual_channel': False
    }
    
    transcript_response = requests.post(
        TRANSCRIPT_ENDPOINT,
        json=transcript_request,
        headers=headers)
    
    if transcript_response.status_code != 200:
        raise Exception(f"Transcription request failed: {transcript_response.text}")
    
    transcript_id = transcript_response.json()['id']
    polling_endpoint = f"{TRANSCRIPT_ENDPOINT}/{transcript_id}"

    # Polling para verificar se a transcrição está pronta
    while True:
        polling_response = requests.get(polling_endpoint, headers=headers)
        polling_response_json = polling_response.json()
        
        if polling_response_json['status'] == 'completed':
            return format_transcription_with_speakers(polling_response_json)
        elif polling_response_json['status'] == 'error':
            raise Exception(f"Transcription failed: {polling_response_json['error']}")
        
        time.sleep(3)

def format_transcription_with_speakers(transcript_data):
    """Formata a transcrição com identificação de falantes"""
    if not transcript_data.get('utterances'):
        return transcript_data.get('text', 'Transcrição não disponível')
    
    formatted_text = []
    for utterance in transcript_data['utterances']:
        speaker = f"Falante {utterance['speaker']}"  # Ou use seus próprios rótulos como [OP] e [ET]
        formatted_text.append(f"{speaker}: {utterance['text']}")
    
    return "\n\n".join(formatted_text)

@app.route('/api/transcriptions', methods=['POST'])
def get_transcriptions():
    """Endpoint para processar e retornar as transcrições com diarização"""
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    
    data = request.get_json()
    folder_id = data.get('folder_id')
    
    if not folder_id:
        return jsonify({"error": "folder_id is required"}), 400
    
    try:
        creds = authenticate_with_oauth2()
        service = build('drive', 'v3', credentials=creds)
        files = list_files_in_folder(service, folder_id)
        
        if not files:
            return jsonify({"error": "No audio files found in the specified folder"}), 404
        
        transcriptions_list = []
        
        for file in files:
            temp_filename = None
            try:
                # Cria um nome de arquivo único na pasta temporária
                temp_filename = os.path.join(TEMP_FOLDER, f"temp_{uuid.uuid4().hex}{os.path.splitext(file['name'])[1]}")
                
                # Baixa e salva o arquivo
                audio_data = download_file(service, file['id'])
                with open(temp_filename, 'wb') as f:
                    f.write(audio_data.getvalue())
                
                # Verifica se o arquivo foi criado corretamente
                if not os.path.exists(temp_filename):
                    raise Exception("Failed to create temporary audio file")
                
                # Faz upload para AssemblyAI
                audio_url = upload_to_assemblyai(temp_filename)
                
                # Transcreve o arquivo com diarização
                transcription = transcribe_with_assemblyai(audio_url)
                
                # Adiciona ao resultado
                transcriptions_list.append({
                    "original_file": file['name'],
                    "file_id": file['id'],
                    "content": transcription,
                    "audio_format": os.path.splitext(file['name'])[1][1:].upper(),
                    "has_speaker_labels": True
                })
                
            except Exception as e:
                transcriptions_list.append({
                    "original_file": file['name'],
                    "file_id": file['id'],
                    "error": str(e)
                })
            finally:
                # Remove o arquivo temporário se existir
                if temp_filename and os.path.exists(temp_filename):
                    try:
                        os.remove(temp_filename)
                    except Exception as e:
                        print(f"Failed to remove temporary file {temp_filename}: {str(e)}")
        
        result = {
            "metadata": {
                "processed_time": datetime.now().isoformat(),
                "service": "AssemblyAI",
                "features": ["diarization", "punctuation", "formatting"],
                "total_files": len(transcriptions_list),
                "successful_transcriptions": len([t for t in transcriptions_list if 'content' in t])
            },
            "transcriptions": transcriptions_list
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
    #um comementario
    