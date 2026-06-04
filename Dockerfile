FROM python:3.14-slim

WORKDIR /app

# Copier les fichiers source
COPY src/*.py /app/

# Définir l'encodage UTF-8
ENV PYTHONIOENCODING=utf-8
ENV PYTHONUNBUFFERED=1

# Exposer le port pour la communication (optionnel, pour extensions futures)
EXPOSE 8080

# Commande par défaut
CMD ["python", "main.py"]