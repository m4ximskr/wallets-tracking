from flask import Flask, jsonify, request
import main

app = Flask(__name__)

@app.route('/analyze-wallet', methods=['GET'])
def analyze_wallet_endpoint():
    try:
        wallet_address = request.args.get('wallet', default='default_wallet', type=str)
        main.transactions = 100
        result = main.requestData(wallet_address)
        print('result', result)
        return jsonify({'result': result}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
