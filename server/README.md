# THE CASINO 対戦サーバー

依存パッケージなし（Node.js 18 以上だけで動きます）。

## 動かし方
フォルダ構成はこのまま（`index.html` と `server/` を同じ階層に置く）:

    index.html
    server/server.js

    node server/server.js          # → http://localhost:3000/
    PORT=8080 node server/server.js

ブラウザで `http://サーバーのアドレス:3000/` を開くと、ゲームが配信され、オンライン対戦は自動でこのサーバーを使います。

## 別の場所でゲームを配信する場合
`index.html` 内の `const SERVER_URL='';` にサーバーのURLを書きます（例: `'wss://casino.example.com/ws'`）。
空のままだと従来どおり PeerJS で対戦します。

## 公開するとき
- HTTPS のページからは `wss://` が必要です。Render / Railway / Fly.io などは HTTPS を自動で付けてくれます。
- 起動コマンドは `node server/server.js`、ポートは環境変数 `PORT`（多くの無料サービスが自動設定）。
- 動作確認用に `/healthz` が `ok` を返します。

## しくみ
サーバーは「合言葉 → 部屋の作成者」の取り次ぎと、メッセージの中継だけをします。
ゲームの進行や所持金の管理は今までどおり各端末が行います（部屋を作った人が中継役）。
