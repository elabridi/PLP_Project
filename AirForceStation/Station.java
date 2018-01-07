
public class Station {

	public static void main(String args) {
		String usaf = args.substring(0, 6);
		String name = args.substring(13, 13+29);
		String fips = args.substring(43, 43+2);
		String altitude = args.substring(74, 74+7);
		System.out.println("usaf : " + usaf + ", name : " + name + ", fips : " + fips + ", altitude : " + altitude);
	}
}
