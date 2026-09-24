package rendezvous

// Alive 报告会合槽是否仍可用（未被 Close、未被 Hub.Cleanup 按 idleTTL 回收）。
// 上层（桥的 provider 侧）据此分辨「会话还在，只是这一轮 HTTP 超时了」与「会话真的没了」——
// 前者留着让客户端重发续跑，后者才该向客户端报错。
func (s *Session) Alive() bool {
	return s != nil && s.ctx != nil && s.ctx.Err() == nil
}
