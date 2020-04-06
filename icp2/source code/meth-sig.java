class Test
{
    public void m1(int i)
    {
        System.out.println("x");
    }
    public void m1(String s){
        System.out.println("Str");
        

    }
    public static void main(String[] args){
        Test t=new Test();
        t.m1(10);
        t.m1("praveen");

    }
}