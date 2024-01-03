pipx install --force poetry &&\
poetry completions bash >> ~/.bash_completion &&\
poetry init &&\
poetry add $(cat requirements.txt)
