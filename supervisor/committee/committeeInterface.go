package committee

import "blockEmulator/message"

type CommitteeModule interface {
	HandleBlockInfo(*message.BlockInfoMsg)
	HandleMixUpdateInfo(msg *message.MixUpdateInfoMsg)
	MsgSendingControl()
	HandleOtherMessage([]byte)
}
