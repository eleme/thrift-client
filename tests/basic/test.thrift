typedef map<string, map<string, i16>> T1

typedef map<S1, string> T2

exception E1 {
  1: required string name;
  2: required string message;
}

struct S1 {
  1: required i32 a;
}

struct Args {
  1: required list<S1> list1;
  2: required T1 map1;
  3: required T2 map2;
}

service Test {

  Args test(1: list<S1> list1, 2: T1 map1, 3: T2 map2) throws (1: E1 exception);

  void void_call();

  binary bin(1: binary data);

  i64 bignumber(1: i64 data);

  bool unknown();

}
