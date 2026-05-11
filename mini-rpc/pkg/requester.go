package minirpc

type RemoteRequester interface {
	CallRemote(serviceMethod string, args any, reply any) error
}
