import java.util.*;

public class Main {
    public static void main(String[] args) {
        Scanner sc=new Scanner(System.in);
        List<Integer> list=new ArrayList<>();

        Deque<Integer> dq=new ArrayDeque<>();
        while(sc.hasNextInt()){
            int num=sc.nextInt();
            list.add(num);
        }
        int n=list.size();
        dq.push(list.get(0));
        for(int i=1;i<n;i++){
            int cur=list.get(i);
            int preSum=0;
            List<Integer> temp=new ArrayList<>();
            boolean flag=false;
            if(dq.isEmpty()||dq.peek()>cur){
                dq.push(cur);
                continue;
            }
            while(!dq.isEmpty()){
                int pre=dq.pop();
                preSum+=pre;
                temp.add(pre);
                if(preSum==cur){
                    flag=true;
                    dq.push(cur*2);
                    break;
                }else if(preSum<cur){
                    continue;
                }else{
                    break;
                }
            }
            if(!flag){
                for(int k=temp.size()-1;k>=0;k--){
                    dq.push(temp.get(k));
                }
                dq.push(cur);
            }


        }
        while(!dq.isEmpty()){
            System.out.print(dq.pop());
            if(!dq.isEmpty()){
                System.out.print(" ");
            }
        }
    }
}
