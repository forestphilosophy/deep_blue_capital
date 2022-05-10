with New_Lasts;
use  New_Lasts;

with Time_Zones;
use  Time_Zones;

with Market_Codes;
use  Market_Codes;

with Sparta.Choosers;
use  Sparta.Choosers;

with Sparta.Dealers_Manager;
use  Sparta.Dealers_Manager;

with Sparta.Kinds;
use  Sparta.Kinds;

with Sparta.Stubs.Types.Ops;
use  Sparta.Stubs.Types.Ops;

with Sparta.Market_Data.Consumers.Stubs;
use  Sparta.Market_Data.Consumers.Stubs;

with Sparta.Market_Data.Phase_Support;
use  Sparta.Market_Data.Phase_Support;

with Sparta.Market_Data.Prices;
use  Sparta.Market_Data.Prices;

with Sparta.Market_Data.Auctions;
use  Sparta.Market_Data.Auctions;

with Sparta.TR.Fid_Fields.Stubs;
use  Sparta.TR.Fid_Fields.Stubs;


package body Sparta.TR.Exchange_Specifics.CME_Futures is
   use Sparta.Market_Data.Lasts;
   use Sparta.Stubs.Types.Duration_T;

   timestamp_auction : constant Ada_Time_T.Target_Stub_Descriptor_Type_Access :=
     B_ACTIV_DATE_L1 + S_QUOTIM_MS;

begin
   Set_Global_Chooser
     (Data_Vendor (TR.Reuters_Data_Vendor));
   -- !!!!!!!!     Add "Register" after this line.     !!!!!!!!!

   Register
     (Mkt (MKT_XCME), 
           Tag => "CME_Futures",
      Descriptors =>
        (TR_Standard_L1_Phase_Dealer                                        
             (Stamp     => Use_First(B_ACTIV_DATE_L1 + S_TIMACT, Received),

              Compute   => Changed (B_PERIOD_CDE),

              Map       =>
                (No_Trading_Phase     => B_PERIOD_CDE <= "4",               
                 Trading_Phase        => B_PERIOD_CDE <= "15" or B_PERIOD_CDE <= "17",          
                 Auction_Phase        => B_PERIOD_CDE <= "21",               
                 Unknown_Phase        => B_PERIOD_CDE <= "18",
                 others               => null)),

         TR_Standard_L1_Price_Dealer_B_Nondeprecated
           (Stamp => Use_First(A_QUODATE, Received),

            Price => (Bid => TR_Construct_Price_L1 (B_BID),
                      Ask => TR_Construct_Price_L1 (B_ASK)),

            Size  => (Bid => AB_BIDSIZE,
                      Ask => AB_ASKSIZE)),

         TR_Standard_L1_Last_Dealer
           (Stamp          => B_ACTIV_DATE + S_SALTIM_NS,                       -- TR_ESZ1_L1.csv.gz 2021-12-05T23:59:59.859271891Z TRADE DATE is 1 day ahead of ACTIV DATE while trade occured on ACTIV DATE => using ACTIV DATE instead
            Last_Stamp     => B_ACTIV_DATE + S_SALTIM_NS,
                                                                            
           Flag_Map       =>                                                
              (Normal_Trade           => S_LSTSALCOND <= "2" and B_PERIOD_CDE <= "17",
               Opening_Auction_Trade  => S_LSTSALCOND <= "2" and B_PERIOD_CDE <= "15" and 
                                         Contained (Time_Of_Day (Received, UTC_TZ),
                                             +"23:00:00", +"23:10:00"),
               Intraday_Auction_Trade => S_LSTSALCOND <= "2" and B_PERIOD_CDE <= "15" and
                                         (Contained (Time_Of_Day (Received, UTC_TZ),
                                          +"04:00:00", +"04:10:00") or
                                          Contained (Time_Of_Day (Received, UTC_TZ),
                                          +"21:30:00", +"21:40:00") or
                                          Contained (Time_Of_Day (Received, UTC_TZ),
                                          +"11:10:00", +"11:20:00")),
               others                 => null),
           Price          => A_TRDPRC_1,
           Quantity       => A_TRDVOL_1,

           Assertions     => (Assert_Currency_L1,
                               Assert_Currency_Blank_Or_Zero_L1,
                              Assert_Currency_Multiplier_L1),                   
           Compute        => A_Last_Compute,                    
                                                                            
           Ignore         => A_L1_Ignore),

         Standard_Aggregated_Last_Dealer
           (Stamp => Received,
            Meta  => A_Meta_L1,
            When_Release =>
              (Auction_Trades => not (Phase <= Auction_Phase),
               Normal_Trade => not (Phase <= Trading_Phase),
               others => null)),

         TR_Standard_L1_Auction_State_Dealer_Nondeprecated                                 
           (Known_Price    => TR_Construct_Price_L1 (B_BID),
            Known_Volumes  => (Bid => Construct_Unsigned_Volume (Info_Volume_Lot_Multiplier * B_BIDSIZE),
                               Ask => Construct_Unsigned_Volume (Info_Volume_Lot_Multiplier * B_ASKSIZE)),
            Akind          =>
              Auction_State_Kind_Type_T_Logic.If_Then_Else
                (not (Phase <= Auction_Phase),
                 Auction_State_Kind_Type_T.Const (None),
                 Auction_State_Kind_Type_T_Logic.If_Then_Else
                   (Is_Valid (B_THEO_OPEN),
                    Auction_State_Kind_Type_T.Const (Known),
                    Auction_State_Kind_Type_T_Logic.If_Then_Else
                      (  Is_Valid (B_BID)
                       and
                         Is_Valid (B_ASK),
                       Auction_State_Kind_Type_T.Const (Non_Tradable),
                       Auction_State_Kind_Type_T.Const (None)))),
            TimeStamp      => Received,
            Exchange_Stamp => timestamp_auction,
            Compute        => A_Compute_Nondeprecated_S
                                  (Target  => Timestamp_Auction,
                                   Compute => A_AucPrice_Compute),

            Assertions     =>
              (Assert_Currency_L1,
               Assert_Currency_Blank_Or_Zero_L1,
               Assert_Currency_Multiplier_L1,
               Assert_B_Auction_Volume_No_Price,
               Assert_A_Auction_Volume_No_Price,
               Assert_Deprecated_AucState_Theo
                 (Target    => timestamp_auction))),


         TR_Standard_L1_Opening_Price_Dealer (Stamp => Use_First(B_ACTIV_DATE_L1 + S_TIMACT,Received),
                                              Date  => B_ACTIV_DATE_L1),
         TR_Standard_L1_Closing_Price_Dealer (Stamp => Use_First(B_ACTIV_DATE_L1 + S_TIMACT,Received),
                                              Date  => B_OFF_CLS_DT),

         TR_L2_By_Price_Dealer                                                  
                   (Summary_Stamp =>                         TR_Monotonize      -- TH_e231575-r727_t9_0x07e7c8864bcd9796_Full.csv.gz 2020-09-28T00:15:09.895589366Z time goes backwards                                 
                          (                                    
                           (TR_Stamp_Refresh_With_Received
                            (B_ACTIV_DATE_L2S+Use_First(S_TIMACT_NS_L2S,S_TIMACT_MS_L2S),
                             B_ACTIV_DATE_L2S+Use_First(B_TIMACT_NS_L2S,B_TIMACT_MS_L2S),
                             B_ACTIV_DATE_L2S+Use_First(S_TIMACT_NS_L2S,S_TIMACT_MS_L2S)))),
                    Entry_Stamp   => S_LV_DATE_L2E + S_LV_TIM_MS_L2E,            

                    Strictness => (Order_Book_Support.Patient with delta         
                              Log_Add => Known_Bug,
                              Log_Delete  => Known_Bug,                     
                              Log_Update  => Known_Bug))                     
        ));                                                                 

   -- !!!!!!!!     Add "Register" before this line.     !!!!!!!!!
   Set_Global_Chooser (null);
end Sparta.TR.Exchange_Specifics.CME_Futures;
