package fj.data;

import static fj.Bottom.errorF;
import static fj.Function.constant;
import static fj.Function.partialApply2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import fj.F;
import fj.Function;
import fj.P;
import fj.P1;
import fj.P2;
import fj.Unit;
import fj.data.Iteratee.Input;
import fj.data.Iteratee.IterV;

public abstract class IO<A> {
  
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

  public static final F<File, IO<BufferedReader>> bufferFile =
    new F<File, IO<BufferedReader>>() {
      @Override
      public IO<BufferedReader> f(final File f) {
        return bufferedReader(f);
      }
    };

  public static final F<Reader, IO<Unit>> closeReader =
    new F<Reader, IO<Unit>>() {
      @Override
      public IO<Unit> f(final Reader r) {
        return closeReader(r);
      }
    };

  public static IO<Unit> closeReader(final Reader r) {
    return new IO<Unit>() {
      @Override
      public Unit run() throws IOException {
        r.close();
        return Unit.unit();
      }
    };
  }

  public static <A> IO<IterV<String, A>> enumFileLines(final File f, final IterV<String, A> i) {
    return bracket(bufferedReader(f)
      , Function.<BufferedReader, IO<Unit>>vary(closeReader)
      , partialApply2(IO.<A>lineReader(), i));
  }

  public static <A> IO<IterV<char[], A>> enumFileChunks(final File f, final IterV<char[], A> i) {
    return bracket(fileReader(f)
      , Function.<Reader, IO<Unit>>vary(closeReader)
      , partialApply2(IO.<A>charChunkReader(), i));
  }

  public static IO<BufferedReader> bufferedReader(final File f) {
    return new IO<BufferedReader>() {
      @Override
      public BufferedReader run() throws IOException {
        return new BufferedReader(new FileReader(f));
      }
    };
  }

  public static IO<Reader> fileReader(final File f) {
    return new IO<Reader>() {
      @Override
      public Reader run() throws IOException {
        return new FileReader(f);
      }
    };
  }

  public static final <A, B, C> IO<C> bracket(final IO<A> init, final F<A, IO<B>> fin, final F<A, IO<C>> body) {
    return new IO<C>() {
      @Override
      public C run() throws IOException {
        final A a = init.run();
        try {
          return body.f(a).run();
        } catch (final IOException e) {
          throw e;
        } finally {
          fin.f(a);
        }
      }
    };
  }

  public static final <A> IO<A> unit(final A a) {
    return new IO<A>() {
      @Override
      public A run() throws IOException {
        return a;
      }
    };
  }

  public static <A> F<BufferedReader, F<IterV<String, A>, IO<IterV<String, A>>>> lineReader() {
    final F<IterV<String, A>, Boolean> isDone =
      new F<Iteratee.IterV<String, A>, Boolean>() {
        final F<P2<A, Input<String>>, P1<Boolean>> done = constant(P.p(true));
        final F<F<Input<String>, IterV<String, A>>, P1<Boolean>> cont = constant(P.p(false));

        @Override
        public Boolean f(final IterV<String, A> i) {
          return i.fold(done, cont)._1();
        }
      };

    return new F<BufferedReader, F<IterV<String, A>, IO<IterV<String, A>>>>() {
      @Override
      public F<IterV<String, A>, IO<IterV<String, A>>> f(final BufferedReader r) {
        return new F<IterV<String, A>, IO<IterV<String, A>>>() {
          final F<P2<A, Input<String>>, P1<IterV<String, A>>> done = errorF("iteratee is done"); //$NON-NLS-1$

          @Override
          public IO<IterV<String, A>> f(final IterV<String, A> it) {
            // use loop instead of recursion because of missing TCO
            return new IO<Iteratee.IterV<String, A>>() {
              @Override
              public IterV<String, A> run() throws IOException {
                IterV<String, A> i = it;
                while (!isDone.f(i)) {
                  final String s = r.readLine();
                  if (s == null) { return i; }
                  final Input<String> input = Input.<String>el(s);
                  final F<F<Input<String>, IterV<String, A>>, P1<IterV<String, A>>> cont = Function.<Input<String>, IterV<String, A>>apply(input).lazy();
                  i = i.fold(done, cont)._1();
                }
                return i;
              }
            };
          }
        };
      }
    };
  }

  public static <A> F<Reader, F<IterV<char[], A>, IO<IterV<char[], A>>>> charChunkReader() {
    final F<IterV<char[], A>, Boolean> isDone =
      new F<Iteratee.IterV<char[], A>, Boolean>() {
        final F<P2<A, Input<char[]>>, P1<Boolean>> done = constant(P.p(true));
        final F<F<Input<char[]>, IterV<char[], A>>, P1<Boolean>> cont = constant(P.p(false));

        @Override
        public Boolean f(final IterV<char[], A> i) {
          return i.fold(done, cont)._1();
        }
      };

    return new F<Reader, F<IterV<char[], A>, IO<IterV<char[], A>>>>() {
      @Override
      public F<IterV<char[], A>, IO<IterV<char[], A>>> f(final Reader r) {
        return new F<IterV<char[], A>, IO<IterV<char[], A>>>() {
          final F<P2<A, Input<char[]>>, P1<IterV<char[], A>>> done = errorF("iteratee is done"); //$NON-NLS-1$

          @Override
          public IO<IterV<char[], A>> f(final IterV<char[], A> it) {
            // use loop instead of recursion because of missing TCO
            return new IO<Iteratee.IterV<char[], A>>() {
              @Override
              public IterV<char[], A> run() throws IOException {
                
                IterV<char[], A> i = it;
                while (!isDone.f(i)) {
                  char[] buffer = new char[DEFAULT_BUFFER_SIZE];
                  final int numRead = r.read(buffer);
                  if (numRead == -1) { return i; }
                  if(numRead < buffer.length) {
                    buffer = Arrays.copyOfRange(buffer, 0, numRead);
                  }
                  final Input<char[]> input = Input.<char[]>el(buffer);
                  final F<F<Input<char[]>, IterV<char[], A>>, P1<IterV<char[], A>>> cont =
                      Function.<Input<char[]>, IterV<char[], A>>apply(input).lazy();
                  i = i.fold(done, cont)._1();
                }
                return i;
              }
            };
          }
        };
      }
    };
  }

  /** An iteratee that consumes chunks of char arrays and returns them as a character stream */
  public static final <E> IterV<char[], Stream<Character>> streamFromChars() {
      final F<Stream<Character>, F<Input<char[]>, IterV<char[], Stream<Character>>>> step =
        new F<Stream<Character>, F<Input<char[]>, IterV<char[], Stream<Character>>>>() {
          final F<Stream<Character>, F<Input<char[]>, IterV<char[], Stream<Character>>>> step = this;

          @Override
          public F<Input<char[]>, IterV<char[], Stream<Character>>> f(final Stream<Character> acc) {
            final P1<IterV<char[], Stream<Character>>> empty =
              new P1<IterV<char[], Stream<Character>>>() {
                @Override
                public IterV<char[], Stream<Character>> _1() {
                  return IterV.cont(step.f(acc));
                }
              };
            final P1<F<char[], IterV<char[], Stream<Character>>>> el =
              new P1<F<char[], IterV<char[], Stream<Character>>>>() {
                @Override
                public F<char[], IterV<char[], Stream<Character>>> _1() {
                  return new F<char[], Iteratee.IterV<char[], Stream<Character>>>() {
                    @Override
                    public IterV<char[], Stream<Character>> f(final char[] e) {
                      return IterV.cont(step.f(acc.append(Stream.fromChars(e))));
                    }
                  };
                }
              };
            final P1<IterV<char[], Stream<Character>>> eof =
              new P1<IterV<char[], Stream<Character>>>() {
                @Override
                public IterV<char[], Stream<Character>> _1() {
                  return IterV.done(acc, Input.<char[]>eof());
                }
              };
            return new F<Input<char[]>, IterV<char[], Stream<Character>>>() {
              @Override
              public IterV<char[], Stream<Character>> f(final Input<char[]> s) {
                return s.apply(empty, el, eof);
              }
            };
          }
        };
      return IterV.cont(step.f(Stream.<Character>nil()));
  }

  /** An iteratee that consumes lines/strings as input elements and returns them as a character stream (lines again separated by \n) */
  public static final <E> IterV<String, Stream<Character>> charStreamFromLines() {
      final F<Stream<Character>, F<Input<String>, IterV<String, Stream<Character>>>> step =
        new F<Stream<Character>, F<Input<String>, IterV<String, Stream<Character>>>>() {
          final F<Stream<Character>, F<Input<String>, IterV<String, Stream<Character>>>> step = this;

          @Override
          public F<Input<String>, IterV<String, Stream<Character>>> f(final Stream<Character> acc) {
            final P1<IterV<String, Stream<Character>>> empty =
              new P1<IterV<String, Stream<Character>>>() {
                @Override
                public IterV<String, Stream<Character>> _1() {
                  return IterV.cont(step.f(acc));
                }
              };
            final P1<F<String, IterV<String, Stream<Character>>>> el =
              new P1<F<String, IterV<String, Stream<Character>>>>() {
                @Override
                public F<String, IterV<String, Stream<Character>>> _1() {
                  return new F<String, Iteratee.IterV<String, Stream<Character>>>() {
                    @Override
                    public IterV<String, Stream<Character>> f(final String e) {
                      return IterV.cont(step.f(acc.append(Stream.single('\n')).append(Stream.fromString(e))));
                    }
                  };
                }
              };
            final P1<IterV<String, Stream<Character>>> eof =
              new P1<IterV<String, Stream<Character>>>() {
                @Override
                public IterV<String, Stream<Character>> _1() {
                  return IterV.done(acc, Input.<String>eof());
                }
              };
            return new F<Input<String>, IterV<String, Stream<Character>>>() {
              @Override
              public IterV<String, Stream<Character>> f(final Input<String> s) {
                return s.apply(empty, el, eof);
              }
            };
          }
        };
      return IterV.cont(step.f(Stream.<Character>nil()));
  }

  public abstract A run() throws IOException;

  public final <B> IO<B> map(final F<A, B> f) {
    return new IO<B>() {
      @Override
      public B run() throws IOException {
        return f.f(IO.this.run());
      }
    };
  }

  public final <B> IO<B> bind(final F<A, IO<B>> f) {
    return new IO<B>() {
      @Override
      public B run() throws IOException {
        return f.f(IO.this.run()).run();
      }
    };
  }
}
