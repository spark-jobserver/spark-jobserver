The binaries in this directory perform no useful computation.

They exist only so that there are valid binaries to use in tests
which require the input to be bytes representing a binary of the correct
type.

The jar contains only the compiled version of:

    public class EmptyClass {
      public static void main(String [] args) {
       System.out.println("This program does nothing");
      }
    }

The egg contains the python package of the following:

    print("This program does nothing")

