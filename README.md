# envcheck
size | select count(*)
-----|----------------
  1day  =   36Mb |
  7days =  184Mb |
 30days =  756Mb |  22-23s
 60days = 1504Mb |  45-46s
 90days = 2252Mb |  134s-135s
365days = 9108Mb |  8m-9m

## 今後直すところ
- [ ] 監視対象がハードコード
- [ ] 引数処理がない
- [ ] didのunique keyだけだと、なぜかselect count(*)が遅い。timeを含めたindexを作ると早くなる。
- [ ] deleteはprimary keyが重要。time, didの順のkeyを作ると良い
- [ ] 複数のスレッドでconnectionを使用するとハング/エラー。cursorを分けるだけでは駄目。
