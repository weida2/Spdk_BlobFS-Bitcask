#include "bitcask.h"

#define db_path "/home/weixiangjiang/spdk_bitcask/bitcask/db/"

int main()
{
    bitcask *db = new bitcask;
    db->Open(db_path);

    db->Put("M2024001", "wzc");
    cout << db->Get("M2024001") << endl;
    db->Close();
    cout << db->Get("M2024001") << endl;
    db->Open(db_path);
    cout << db->Get("M2024001") << endl;
    cout << db->Get("M2024002") << endl;
    db->Show();
    return 0;
}
