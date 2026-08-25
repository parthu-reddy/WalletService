sed -i '' '/@Test/i\
    @Autowired Environment env;\
    @Test public void printEnv() { System.out.println("EXCLUDES=" + env.getProperty("spring.autoconfigure.exclude")); }\
' /Users/parthureddy/Documents/Food\ Delivery.nosync/WalletService/src/test/java/com/fooddelivery/wallet/OpenApiGenerationTest.java
