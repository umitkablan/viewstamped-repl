#include "coresm.hpp"

#include <iostream>
using std::cout;
using std::endl;

namespace vsrepl {

VSREngineCore::VSREngineCore(int totreplicas, int replica)
  : latest_healthtick_received_(1)
  , healthcheck_tick_(1)
  , totreplicas_(totreplicas)
  , replica_(replica)
  , view_(0)
  , status_(Status::Normal)
  , trackDups_SVCs_(totreplicas)
  , trackDups_DVCs_(totreplicas)
{
}

std::variant<std::monostate, VSREngineCore::PrepareMsgsType, VSREngineCore::SVCMsgsType>
VSREngineCore::HealthTimeoutTicked()
{
  std::variant<std::monostate, VSREngineCore::PrepareMsgsType, VSREngineCore::SVCMsgsType> ret;

  if (replica_ == (view_ % totreplicas_)) { // The leader
    PrepareMsgsType arr;
    arr.reserve(totreplicas_-1);
    for (int i = 0; i < totreplicas_; ++i) {
      if (i != replica_)
        arr.push_back(std::make_pair(i, MsgPrepare { view_, -1, -1, "" }));
    }
    ret = std::move(arr);
    return ret;

  }
  // A follower

  ++healthcheck_tick_;
  auto diff = healthcheck_tick_ - latest_healthtick_received_;
  if (healthcheck_tick_ > latest_healthtick_received_ && diff > 2) {
    // cout << replica_ << ":" << view_ << " -> sensed isolated leader" << endl;
    if (diff < 4 || (diff > 5 && !(diff % 8))) {
      // cout << "#" << view_ << "\n";
      SVCMsgsType arr;
      arr.reserve(totreplicas_);
      for (int i = 0; i < totreplicas_; ++i) {
          arr.push_back(std::make_pair(i, MsgStartViewChange { view_ + 1 }));
      }
      ret = std::move(arr);
    }
  }

  return ret;
}

std::variant<std::monostate, std::pair<int, MsgDoViewChange>, VSREngineCore::SVCMsgsType>
VSREngineCore::SVCReceived(int from, const MsgStartViewChange& svc)
{
  std::variant<std::monostate, std::pair<int, MsgDoViewChange>, VSREngineCore::SVCMsgsType> ret;

  auto [isdup, idx] = checkDuplicate(trackDups_SVCs_, from, svc.view);
  if (isdup)
    return ret;
  // cout << replica_ << ":" << view_ << " (SVC) v:" << svc.view << endl;

  auto cnt = std::count(
    trackDups_SVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_SVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt > totreplicas_ / 2) { // don't include self
    status_ = Status::Change;
    view_ = svc.view;
    healthcheck_tick_ = latest_healthtick_received_ - 1;
    ret = std::make_pair(svc.view % totreplicas_, MsgDoViewChange { svc.view });
  } else if (svc.view == view_ + 1
      && healthcheck_tick_ > latest_healthtick_received_
      && healthcheck_tick_ - latest_healthtick_received_ < 3) {
    SVCMsgsType arr;
    arr.reserve(totreplicas_);
    for (int i = 0; i < totreplicas_; ++i) {
        arr.push_back(std::make_pair(i, MsgStartViewChange { view_ + 1 }));
    }
    ret = std::move(arr);
  }

  return ret;
}

std::vector<std::pair<int, MsgStartView>>
VSREngineCore::DVCReceived(int from, const MsgDoViewChange& dvc)
{
  std::vector<std::pair<int, MsgStartView>> ret;

  auto [isdup, idx] = checkDuplicate(trackDups_DVCs_, from, dvc.view);
  if (isdup)
    return ret;
  // cout << replica_ << ":" << view_ << " (DoVC) v:" << dvc.view << endl;

  auto cnt = std::count(
    trackDups_DVCs_.recv_replicas_.begin()+(idx*totreplicas_),
    trackDups_DVCs_.recv_replicas_.begin()+(idx+1)*totreplicas_,
    1);

  if (cnt >= totreplicas_ / 2) { // include self
    view_ = dvc.view;
    status_ = Status::Normal;

    ret.reserve(totreplicas_-1);
    for (int i = 0; i < totreplicas_; ++i) {
      if (i != replica_)
        ret.push_back(std::make_pair(i, MsgStartView { dvc.view }));
    }
  }

  return ret;
}

int VSREngineCore::SVReceived(int from, const MsgStartView& svc)
{
  // cout << replica_ << ":" << view_ << " (SV) v:" << svc.view << endl;
  if (view_ < svc.view) {
    cout << replica_ << ":" << view_ << " (SV) my view is smaller than received v:" << svc.view << endl;
  }

  if (view_ <= svc.view) {
    healthcheck_tick_ = latest_healthtick_received_;
    view_ = svc.view;
    status_ = Status::Normal;
  } else
    cout << replica_ << ":" << view_ << " (SV) my view is bigger than received v:" << svc.view << "!! skipping..." << endl;

  return 0;
}

int VSREngineCore::PrepareReceived(const MsgPrepare& pr)
{
  // cout << replica_ << ":" << view_ << " (PREP) v:" << pr.view << endl;
  if (view_ < pr.view) {
    cout << replica_ << ":" << view_ << " (PREP) I am OUTDATED v:" << pr.view << endl;
    view_ = pr.view;
    status_ = Status::Normal;
  } else if (view_ > pr.view) {
    cout << replica_ << ":" << view_ << " (PREP) Skipping old v:" << pr.view << endl;
    return 17;
  }

  if (status_ == Status::Normal) {
    if (replica_ != view_ % totreplicas_) {
        healthcheck_tick_ = latest_healthtick_received_;
    }
  }
  return 0;
}

std::pair<bool,int>
VSREngineCore::checkDuplicate(VSREngineCore::trackDups &td, int from, int view)
{
  auto find_from = [this, from, &td]() {
    for(int i=0; i<totreplicas_; ++i) {
      if (td.recv_replicas_[i * totreplicas_ + from])
        return i;
    }
    return -1;
  };
  auto find_view = [this, view, &td]() {
    std::pair<int,int> ret{-1,-1};
    for (int i = 0; i < totreplicas_; ++i) {
      if (td.recv_views_[i] == view) {
          ret.first = i;
          break;
      }
    }
    if (ret.first == -1)
      for(int i=0; i<totreplicas_; ++i)
        if (td.recv_views_[i] == -1) {
          ret.second = i;
          break;
        }
    return ret;
  };

  auto fromi = find_from();
  if (fromi != -1) {
    if (view == td.recv_views_[fromi])
      return std::make_pair(true, fromi);

      td.recv_replicas_[fromi * totreplicas_ + from] = 0; // clear previous view's recv record
      if (std::all_of(
        td.recv_replicas_.begin() + fromi*totreplicas_, td.recv_replicas_.begin() + (fromi+1)*totreplicas_,
        [](int v) { return v == 0; }))
        td.recv_views_[fromi] = -1;
  }

  auto [viewi, emptyi] = find_view();
  if (viewi == -1) {
    if (emptyi == -1) // impossible to not-find empty solution, just for completeness
      throw std::invalid_argument(std::to_string(replica_) + ":" + std::to_string(view_) + " (checkDupSVC) from:" + std::to_string(from) + " view:" + std::to_string(view));
    td.recv_views_[emptyi] = view;
    td.recv_replicas_[emptyi * totreplicas_ + from] = 1;
    return std::make_pair(false, emptyi);
  }

  bool ret = td.recv_replicas_[viewi * totreplicas_ + from];
  td.recv_replicas_[viewi * totreplicas_ + from] = 1;
  return std::make_pair(ret, viewi);
}

}
