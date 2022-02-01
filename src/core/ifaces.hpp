#ifndef TSTAMPED_FEED_INCLUDED_
#define TSTAMPED_FEED_INCLUDED_ 1

#include "msgs.hpp"

#include <string>

namespace vsrepl {

class IDispatcher {
public:
  virtual ~IDispatcher() {}

  virtual void SendMsg(int to, const MsgClientOp&) = 0;
  virtual void SendMsg(int to, const MsgStartViewChange&) = 0;
  virtual void SendMsg(int to, const MsgDoViewChange&) = 0;
  virtual void SendMsg(int to, const MsgStartView&) = 0;
  virtual void SendMsg(int to, const MsgPrepare&) = 0;
  virtual void SendMsg(int to, const MsgGetMissingLogs&) = 0;

  // Necessary for Client
  virtual void SendMsg(     int to, const MsgOpPersistedQuery&) = 0;
  virtual void SendToClient(int to, const MsgPersistedCliOp&) = 0;
};

class INetDispatcher {
public:
  virtual ~INetDispatcher() {}

  virtual void SendMsg(int from, int to, const MsgClientOp&) = 0;
  virtual void SendMsg(int from, int to, const MsgStartViewChange&) = 0;
  virtual void SendMsg(int from, int to, const MsgDoViewChange&) = 0;
  virtual void SendMsg(int from, int to, const MsgStartView&) = 0;
  virtual void SendMsg(int from, int to, const MsgPrepare&) = 0;
  virtual void SendMsg(int from, int to, const MsgGetMissingLogs&) = 0;

  // Necessary for Client
  virtual void SendMsg(     int from, int to, const MsgOpPersistedQuery&) = 0;
  virtual void SendToClient(int from, int to, const MsgPersistedCliOp&) = 0;
};

class IStateMachine {
public:
  virtual ~IStateMachine() {};

  virtual int Execute(const std::string& opstr) = 0;
};

}

#endif
