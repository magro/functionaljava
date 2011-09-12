package fj.data;

import static fj.Bottom.errorF;
import static fj.Function.constant;
import static fj.Function.partialApply2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import fj.F;
import fj.Function;
import fj.P;
import fj.P1;
import fj.P2;
import fj.Unit;
import fj.data.Iteratee.Input;
import fj.data.Iteratee.IterV;

public abstract class IO<A> {

  public static final F<File, IO<BufferedReader>> bufferFile =
    new F<File, IO<BufferedReader>>() {
      @Override
      public IO<BufferedReader> f(final File f) {
        return bufferFile(f);
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

  public static void main(final String[] args) {
    final File f = new File("/tmp/test.txt");
    try {
      System.err.println(enumFile(f, IterV.<String>length()).run().run());
      final IterV<String, Option<String>> head = IterV.<String>head();
      System.err.println(enumFile(f, head).run().run());
      System.err.println(enumFile(f, IterV.<String>drop(2).bind(Function.<Unit, IterV<String, Option<String>>>constant(head))).run().run());
      System.err.println(enumFile(f, IterV.<String>peek()).run().run());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <A> IO<IterV<String, A>> enumFile(final File f, final IterV<String, A> i) {
    return bracket(bufferFile(f)
      , Function.<BufferedReader, IO<Unit>>vary(closeReader)
      , partialApply2(IO.<A>enumReader(), i));
  }

  public static IO<BufferedReader> bufferFile(final File f) {
    return new IO<BufferedReader>() {
      @Override
      public BufferedReader run() throws IOException {
        return new BufferedReader(new FileReader(f));
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

  public static <A> F<BufferedReader, F<IterV<String, A>, IO<IterV<String, A>>>> enumReader() {
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
