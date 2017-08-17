#pragma once

namespace opossum {

namespace static_if_detail {

template<bool Cond>
struct Statement {
  template<typename Functor>
  void then(const Functor &func){
    func();
  }

  template<typename Functor>
  void else_(const Functor&){}
};

template<>
struct Statement<false> {
  template<typename Functor>
  void then(const Functor&){}

  template<typename Functor>
  void else_(const Functor& func){
    func();
  }
};

}  // namespace static_if_detail

template<bool Cond, typename Functor>
static_if_detail::Statement<Cond> static_if(const Functor &func){
  static_if_detail::Statement<Cond> if_{};
  if_.then(func);
  return if_;
}

}  // namespace opossum