# 環境変数の読み込み
source ~/.aws/credencials

# virtualenvの設定
export env_name="blogentry-twitternazo-random"
virtualenv ${env_name}
source  ${env_name}/bin/activate

# pipインストール
pip install -r requirements.txt
pip install lamvery
# lamvery deploy
