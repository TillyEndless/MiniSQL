#include "buffer/clock_replacer.h"
#include "gtest/gtest.h"

/**
 * 测试目的：
 * 1. Unpin 将 frame 加入环并设置参考位
 * 2. Victim 会跳过参考位=1 的 frame（清 1），直到遇到参考位=0 的
 * 3. Pin 会将 frame 从环中移除
 * 4. Unpin 重复同一个 frame 时不重复加入
 * 5. 环满之后再 Unpin 不增加容量
 * 6. pointer 在 wrap-around 后依然正确
 */
TEST(CLOCKReplacerTest, ComprehensiveScenarios) {
  const size_t CAP = 4;
  CLOCKReplacer clock(CAP);

  frame_id_t fid;

  EXPECT_EQ(0U, clock.Size());
  EXPECT_FALSE(clock.Victim(&fid));

  clock.Unpin(0);
  clock.Unpin(1);
  clock.Unpin(2);
  clock.Unpin(3);
  EXPECT_EQ(CAP, clock.Size());

  // ——————————————————————————————————————————————
  // 4
  clock.Unpin(1);
  EXPECT_EQ(CAP, clock.Size());

  // ——————————————————————————————————————————————
  // Victim测试
  //    第一次 Victim：手臂指向 0，ref=1 → 清为 0，跳过
  //    再手臂指向 1，ref=1 → 清为 0，跳过
  //    再指向 2，ref=1 → 清为 0，跳过
  //    再指向 3，ref=1 → 清为 0，跳过
  //    再指回 0（ref=0） → 选中 0
  EXPECT_TRUE(clock.Victim(&fid));
  EXPECT_EQ(0, fid);
  EXPECT_EQ(3U, clock.Size());

  // ——————————————————————————————————————————————
  // 2次Victim
  EXPECT_TRUE(clock.Victim(&fid));
  EXPECT_EQ(1, fid);
  EXPECT_EQ(2U, clock.Size());

  // ——————————————————————————————————————————————
  // Pin
  clock.Pin(2);
  EXPECT_EQ(1U, clock.Size());

  // ——————————————————————————————————————————————
  // Unpin(2) → 重新加入环，ref置1
  clock.Unpin(2);
  EXPECT_EQ(2U, clock.Size());

  // ——————————————————————————————————————————————
  // Victim
  EXPECT_TRUE(clock.Victim(&fid));
  EXPECT_EQ(3, fid);
  EXPECT_EQ(1U, clock.Size());

  // ——————————————————————————————————————————————
  // Victim
  EXPECT_TRUE(clock.Victim(&fid));
  EXPECT_EQ(2, fid);
  EXPECT_EQ(0U, clock.Size());

  // ——————————————————————————————————————————————
  // 空环Victim
  EXPECT_FALSE(clock.Victim(&fid));

  // ——————————————————————————————————————————————
  // 尝试添加超过 CAP 个
  for (int i = 0; i < static_cast<int>(CAP) + 2; i++) {
    clock.Unpin(i);
  }
  EXPECT_EQ(CAP, clock.Size());

  // 清空环
  while (clock.Victim(&fid)) {}
  EXPECT_EQ(0U, clock.Size());
}
