/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <time.h>

#include <gmp.h>


time_t printTime(char* label)
{
  time_t seconds;
  struct tm * timeinfo;

  time(&seconds);
  timeinfo = localtime(&seconds);
  printf("%s: %s\n", label, asctime(timeinfo));
  return seconds;
}


int main()
{
  time_t starttime, duration;
  starttime = printTime("START");
  printf("GMP %d.%d.%d\n", __GNU_MP_VERSION, __GNU_MP_VERSION_MINOR, __GNU_MP_VERSION_PATCHLEVEL);

  gmp_randstate_t state;
  gmp_randinit_default(state);
  mp_bitcnt_t n = 1 << 20;

  mpz_t a;
  mpz_t b;
  mpz_t c;

  mpz_init(a);
  mpz_init(b);
  mpz_init(c);

  int i = 0;
  for(; i < 64; i++) 
  {
    if ((i & 0xF) == 0)
      printf("i = %d\n", i);

  mpz_urandomb(a, state, n);
//  printf("a = ");
//  mpz_out_str(stdout, 16, a);
//  printf("\n");

  mpz_urandomb(b, state, n);
//  printf("b = ");
//  mpz_out_str(stdout, 16, b);
//  printf("\n");

  mpz_mul(c,a,b);
//  printf("c = ");
//  mpz_out_str(stdout, 16, c);
//  printf("\n");
  }

  duration = printTime("DONE") - starttime;
  printf("%ld seconds\n", duration);
}
