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
#include <stdlib.h>
#include <time.h>

#include <gmp.h>

#define VERSION "20110122b"

#define OUT stderr

time_t starttime, duration;

time_t printTime(FILE* out, const char* label)
{
  time_t seconds;
  struct tm * timeinfo;

  time(&seconds);
  timeinfo = localtime(&seconds);
  fprintf(out, "%s: %s", label, asctime(timeinfo));
  return seconds;
}

void printZ(FILE* out, const int base, const mpz_t x, const char * name)
{
  /*
  fprintf(out, "%s = ", name);
  mpz_out_str(out, base, x);
  fprintf(out, "\n");
  */
}

void printOutput(FILE* out, const int base, const mpz_t x, const char * name)
{
  printZ(OUT, base, x, name);
  fprintf(stdout, "%ld\n", mpz_sizeinbase(x, base));
  mpz_out_str(stdout, base, x);
  fprintf(stdout, "\n");
  fflush(stdout);
}

// GMP multiplication
int gmpMult(const int base)
{
  mpz_t x;
  mpz_t y;
  mpz_t z;

  mpz_init(x);
  mpz_init(y);
  mpz_init(z);

  int i = 1;
  for(;;) 
  {
    const size_t xbytes = mpz_inp_str(x, stdin, base);
    if (xbytes == 0) {
      return 0;
    }

    const size_t ybytes = mpz_inp_str(y, stdin, base);
    if (ybytes == 0) {
      fprintf(OUT, "ERROR: x without y");
      return 1;
    }

    mpz_mul(z, x, y);
    printOutput(OUT, base, z, "x*y");

    if ((++i & 0xFF) == 0)
    {
      fprintf(OUT, "xbytes=%zu, ybytes=%zu\n", xbytes, ybytes);
    }
  }
}

int main(int argc, char *argv[])
{
  starttime = printTime(OUT, "\nSTART");
  fprintf(OUT, "VERSION=%s, GMP%d.%d.%d\n", VERSION,
      __GNU_MP_VERSION, __GNU_MP_VERSION_MINOR, __GNU_MP_VERSION_PATCHLEVEL);

  // parse arguments
  fprintf(OUT, "\nargc=%d, argv=[%s", argc, argv[0]);
  int i = 1;
  for(; i < argc; i++) 
    fprintf(OUT, ", %s", argv[i]);
  fprintf(OUT, "]\n");
  if (argc != 2)
  {
    fprintf(stderr, "\nUsage: %s <base>\n", argv[0]);
    exit(1);
  }

  // GMP multiplication
  const int base = atoi(argv[1]);
  const int r = gmpMult(base);
  
  duration = printTime(OUT, "DONE") - starttime;
  fprintf(OUT, "%ld seconds\n", duration);
  return r;
}

