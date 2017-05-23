


class migrate_fn_type{
  public:
     virtual ~migrate_fn_type(){}
     virtual void migrate(void*　src, void* dst, int size) const noexcept = 0 ;
}



