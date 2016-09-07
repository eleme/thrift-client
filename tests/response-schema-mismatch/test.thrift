struct S1 {
  1: required i32 a;
}

service Test {

  S1 a();

  list<i32> b();

}
